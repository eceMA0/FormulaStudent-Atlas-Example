using System;
using System.Collections.Generic;
using System.Linq;

using Google.Protobuf;

using MA.DataPlatforms.Streaming.Support.Lib.Core.Contracts;
using MA.DataPlatforms.Streaming.Support.Lib.Core.Contracts.DataFormatInfoModule;
using MA.DataPlatforms.Streaming.Support.Lib.Core.Contracts.SessionInfoModule;
using MA.DataPlatforms.Streaming.Support.Lib.Core.Contracts.SessionInfoModule.Abstractions;
using MA.DataPlatforms.Streaming.Support.Lib.Core.Contracts.WritingModule;
using MA.DataPlatforms.Streaming.Support.Lib.Core.Shared.Abstractions;
using MA.Streaming.OpenData;
using MA.Streaming.API;

namespace WriterCSharp;

// Replays a logged run onto the streaming platform so it can be viewed in ATLAS.
// Creates a session, publishes a configuration packet describing every channel in the CSV, then
// streams the logged samples as periodic data packets before ending the session.
internal class MockDataWriter
{
    private const string ApplicationName = "UCLR"; // CHANGE FOR YOUR TEAM. Will be used to create a session name in ATLAS

    private readonly IPacketWriterService packetWriterService;
    private readonly IDataFormatManagementService dataFormatManagementService;
    private readonly ISessionManagementService sessionManagementService;
    private readonly ILogger logger;
    private readonly PacketIdGenerator packetIdGenerator = new();
    private readonly Dictionary<string, ParameterMetadata> metadata;

    // "" is the default/unnamed stream - it maps to the bare "Data.{DataSource}" topic. e.g. Data.Default)
    private readonly string[] streams = { "", "Stream1" };

    private readonly CsvDataSource csvSource;
    private readonly string[] parameterIdentifiers;

    // (IN CASE YOU HAVE AN ASSOCIATE SESSION. NOT INCLUDED BY DEFAULT IN THIS EXAMPLE)
    // Identifier the associate (Python) writer must use so this main writer can recognise it. 
    private const string AssociateIdentifier = "PythonAssociateSession";

    private readonly HashSet<string> associatedKeys = new();

    // Set once the main session ends, so a late-arriving associate is not linked to a dead session.
    private volatile bool mainSessionEnded;

    public MockDataWriter(
        IPacketWriterService packetWriterService,
        IDataFormatManagementService dataFormatManagementService,
        ISessionManagementService sessionManagementService,
        ILogger logger,
        CsvDataSource csvSource,
        Dictionary<string, ParameterMetadata> metadata)
    {
        this.packetWriterService = packetWriterService;
        this.dataFormatManagementService = dataFormatManagementService;
        this.sessionManagementService = sessionManagementService;
        this.logger = logger;
        this.csvSource = csvSource;
        this.metadata = metadata;
        this.parameterIdentifiers = csvSource.ParameterNames
            .Select(name => $"{name}:{ApplicationName}")
            .ToArray();
    }

    public void CreateStartWriteAndEndMockSession()
    {
        // 1. Build the SessionCreationDto (dataSource="Default", identifier, type, version, UTC offset).
        // 2. Call this.CreateSession(...), validate ApiResult.Success/Data, keep sessionInfo.
        // 3. Call this.StartSession(sessionInfo) to send NewSessionPacket to each stream.
        // 4. Build the config packet via this.CreateConfigPacket() and send it to every stream
        //    via this.CreateAndSendPacket(...), validate all succeeded.
        // 5. Call this.dataFormatManagementService.GetParameterDataFormatId("Default", this.parameterIdentifiers),
        //    validate, keep dataFormatId.
        // 6. Send an "Out Lap" MarkerPacket to every stream.
        // 7. Optionally send an updated SessionInfoPacket via this.CreateAndSendSessionInfoPacket(...).
        // 8. Loop: use a PeriodicPacketGenerator to GeneratePackets() and send them via
        //    this.CreateAndSendPacket(...) to "Stream1", until Ctrl+C is caught.
        // 9. Call this.EndSession(sessionInfo.DataSource, sessionInfo.SessionKey).
        var sessionCreationDto = new SessionCreationDto(
            dataSource: "Default",
            identifier: this.csvSource.Name,
            type: "Session",
            version: 1,
            utcOffset: DateTimeOffset.Now.Offset);
        var sessionCreationResult = this.CreateSession(sessionCreationDto);
        var sessionInfo = sessionCreationResult.Data;
        if (!sessionCreationResult.Success || sessionInfo == null) {
            this.logger.Error($"Failed to create session: {sessionCreationResult.Message}");
            return;
        }
        this.StartSession(sessionInfo);
        this.BeginWatchingForAssociateSessions(sessionInfo);
        var configurationPacket = this.CreateConfigPacket();
        var success = true;
        foreach (var stream in this.streams)
        {
            var configPacket = new Packet
            {
                Type = "Configuration",
                SessionKey = sessionInfo.SessionKey,
                IsEssential = true,
                Content = configurationPacket.ToByteString(),
                Id = this.packetIdGenerator.GetPacketId(),
            };
            success &= this.CreateAndSendPacket(configPacket, sessionInfo.DataSource, sessionInfo.SessionKey, stream);
        }
        if (!success)
        {
            this.logger.Error("Failed to send config packet to all streams");
            this.EndSession(sessionInfo.DataSource, sessionInfo.SessionKey);
            return;
        }
        var dataFormatIdResult = this.dataFormatManagementService.GetParameterDataFormatId("Default", this.parameterIdentifiers);
        if (dataFormatIdResult.Data == null || !dataFormatIdResult.Success)
        {
            this.logger.Error($"Failed to get data format ID: {dataFormatIdResult.Message}");
            this.EndSession(sessionInfo.DataSource, sessionInfo.SessionKey);
            return;
        }
        var dataFormatId = dataFormatIdResult.Data.DataFormatId;
        
        var firstTimeStamp = (ulong)(DateTime.UtcNow.Ticks - DateTime.UnixEpoch.Ticks) * 100;
        var lapPacket = new MarkerPacket
        {
            Timestamp = firstTimeStamp,
            Label = "Out Lap",
            Value = 1,
            Source = "0",
            Type = "Lap Trigger",
            Description = "Out Lap Marker",
        };
        for (var i = 0; i < this.streams.Length; i++)
        {
            success &= this.CreateAndSendPacket(
                new Packet
                {
                    Type = "Marker",
                    SessionKey = sessionInfo.SessionKey,
                    IsEssential = false,
                    Content = lapPacket.ToByteString(),
                    Id = this.packetIdGenerator.GetPacketId(),

                },
                sessionInfo.DataSource,
                sessionInfo.SessionKey,
                this.streams[i]);
        }

        // It's possible to update session information after the session is created.
        var newSessionDetail = new SessionInfoPacket
        {
            DataSource = sessionInfo.DataSource,
            Identifier = sessionInfo.Identifier,
            Type = sessionInfo.Type,
            Version = sessionInfo.Version,
            Details = { { "Test Detail", "Test Value" } },
        };
        this.CreateAndSendSessionInfoPacket(
            new Packet
            {
                Type = "SessionInfo",
                SessionKey = sessionInfo.SessionKey,
                IsEssential = false,
                Content = newSessionDetail.ToByteString(),
                Id = this.packetIdGenerator.GetPacketId(),
            });

        var periodicPacketGenerator = new PeriodicPacketGenerator(this.csvSource, dataFormatId, firstTimeStamp);
        var cancelled = false;
        Console.CancelKeyPress += (sender, args) =>
        {
            args.Cancel = true;
            cancelled = true;
        };

        this.logger.Info(
            $"Replaying {this.csvSource.Rows.Count} rows of '{this.csvSource.Name}' " +
            $"({this.csvSource.ParameterNames.Length} channels at {this.csvSource.Frequency}Hz)...");

        // Stop as soon as the CSV is exhausted so the session closes and Atlas sees a complete historic run.
        while (!cancelled)
        {
            var periodicPacket = periodicPacketGenerator.GeneratePackets();
            if (periodicPacket == null)
            {
                this.logger.Info("Reached the end of the CSV, ending session...");
                break;
            }

            foreach (var stream in this.streams)
            {
                this.CreateAndSendPacket(
                    new Packet
                    {
                        Type = "PeriodicData",
                        SessionKey = sessionInfo.SessionKey,
                        IsEssential = false,
                        Content = periodicPacket.ToByteString(),
                        Id = this.packetIdGenerator.GetPacketId(),
                    },
                    sessionInfo.DataSource,
                    sessionInfo.SessionKey,
                    stream);
            }
        }

        if (cancelled)
        {
            this.logger.Info("Ctrl+C detected, ending session...");
        }

        this.mainSessionEnded = true;
        this.EndSession(sessionInfo.DataSource, sessionInfo.SessionKey);
    }

    // This writer is the MAIN session. Associate sessions (e.g. the Python writer) are discovered by
    // identifier as they come online, then linked to this session via AddAssociateSession.
    private void BeginWatchingForAssociateSessions(ISessionInfo mainSession)
    {
        this.sessionManagementService.LiveSessionStarted += (_, startedSession) =>
            this.TryAssociate(mainSession, startedSession);

        // Link any associate session that was already live before we subscribed.
        var existing = this.sessionManagementService.GetAllSessions();
        if (existing.Success && existing.Data != null)
        {
            foreach (var candidate in existing.Data)
            {
                this.TryAssociate(mainSession, candidate);
            }
        }
    }

    private void TryAssociate(ISessionInfo mainSession, ISessionInfo candidate)
    {
        if (this.mainSessionEnded)
        {
            return;
        }

        if (candidate.DataSource != mainSession.DataSource ||
            candidate.Identifier != AssociateIdentifier ||
            candidate.SessionKey == mainSession.SessionKey)
        {
            return;
        }
        if (candidate.Historical)
        {
            return;
        }

        lock (this.associatedKeys)
        {
            if (!this.associatedKeys.Add(candidate.SessionKey))
            {
                return;
            }
        }

        var result = this.sessionManagementService.AddAssociateSession(
            mainSession.DataSource,
            mainSession.SessionKey,
            candidate.SessionKey);

        if (result.Success && result.Data != null)
        {
            this.logger.Info(
                $"Associated session {candidate.SessionKey} ('{candidate.Identifier}') to main session {mainSession.SessionKey}. " +
                $"Total associates: {result.Data.AssociateSessionKeys.Count}");
        }
        else
        {
            this.logger.Error($"Failed to associate session {candidate.SessionKey}: {result.Message}");
            lock (this.associatedKeys)
            {
                this.associatedKeys.Remove(candidate.SessionKey);
            }
        }
    }


    // Builds a list of timestamps starting at firstTimestamp, spaced according to frequency.
    private static List<long> CreateTimestamps(int numberOfSamples, long firstTimestamp, double frequency)
    {
        long[] timestamps = new long[numberOfSamples];
        var period = 1e9 / frequency;
        for (int i = 0;i < numberOfSamples; i++)
        {
            timestamps[i] = (long)(firstTimestamp + i * period);
        }
        return timestamps.ToList();
    }

    // Builds the ConfigurationPacket describing every channel found in the CSV.
    private ConfigurationPacket CreateConfigPacket()
    {
        var configurationPacket = new ConfigurationPacket
        {
            GroupDefinitions =
            {
                new GroupDefinition
                {
                    Identifier = ApplicationName,
                    Name = ApplicationName,
                    ApplicationName = ApplicationName,
                    Description = ApplicationName,
                }
            },
        };


        var frequency = (uint)Math.Round(this.csvSource.Frequency);
        for (var i = 0; i < this.csvSource.ParameterNames.Length; i++)
        {
            var name = this.csvSource.ParameterNames[i];
            var range = this.GetChannelRange(i);

            // Anything supplied in the metadata file wins; everything else falls back to a value
            // derived from the CSV itself, so a partial metadata file is still valid.
            this.metadata.TryGetValue(name, out var overrides);

            configurationPacket.ParameterDefinitions.Add(
                new ParameterDefinition
                {
                    Identifier = this.parameterIdentifiers[i],
                    ApplicationName = ApplicationName,
                    Name = name,
                    FormatString = overrides?.FormatString ?? "%5.2f",
                    DataType = DataType.Float64,
                    Units = overrides?.Units ?? string.Empty,
                    MinValue = overrides?.MinValue ?? range.Min,
                    MaxValue = overrides?.MaxValue ?? range.Max,
                    WarningMinValue = overrides?.MinValue ?? range.Min,
                    WarningMaxValue = overrides?.WarningMaxValue ?? range.Max,
                    Frequencies = { frequency },
                    IncludesSynchroData = false,
                    IncludesRowData = false,
                    Description = overrides?.Description ?? name,
                });
        }

        configurationPacket.ConfigId = BuildConfigId(configurationPacket);

        return configurationPacket;
    }

    private static string BuildConfigId(ConfigurationPacket configurationPacket)
    {
        var hash = System.Security.Cryptography.SHA256.HashData(configurationPacket.ToByteArray());
        return $"{ApplicationName}-{Convert.ToHexString(hash)[..16]}";
    }

    // Atlas uses min/max to scale a channel's axis. Scanning the logged values gives a sensible
    // range per channel instead of the same arbitrary 0-50 for every one of them.
    private (double Min, double Max) GetChannelRange(int columnIndex)
    {
        var min = double.MaxValue;
        var max = double.MinValue;

        foreach (var row in this.csvSource.Rows)
        {
            var value = row[columnIndex];
            if (!value.HasValue)
            {
                continue;
            }

            min = Math.Min(min, value.Value);
            max = Math.Max(max, value.Value);
        }

        // A channel that is entirely "####", or one that never changes, still needs a non-zero span.
        if (min > max)
        {
            return (0.0, 1.0);
        }

        return Math.Abs(max - min) < double.Epsilon ? (min, min + 1.0) : (min, max);
    }

    // Serializes and writes a data packet to the given stream. Returns whether the write succeeded.
    private bool CreateAndSendPacket(Packet packet, string dataSource, string sessionKey, string stream)
    {

        var result = this.packetWriterService.WriteData(dataSource, stream, sessionKey, packet);
        return result.Success;
    }

    // Serializes and writes a SessionInfo packet. Returns whether the write succeeded.
    private bool CreateAndSendSessionInfoPacket(Packet packet)
    {
        
        var result = this.packetWriterService.WriteInfo(packet, InfoType.SessionInfo);
        return result.Success;
    }

    // Creates a new session via the session management service.
    private ApiResult<ISessionInfo?> CreateSession(SessionCreationDto sessionCreationDto)
    {

        return this.sessionManagementService.CreateNewSession(sessionCreationDto);
    }

    // Sends a NewSessionPacket to every stream to mark the start of the session.
    private void StartSession(ISessionInfo sessionInfo)
    {

        var newSessionPacket = new NewSessionPacket
        {
            DataSource = sessionInfo.DataSource,
            UtcOffset = Google.Protobuf.WellKnownTypes.Duration.FromTimeSpan(sessionInfo.UtcOffset),
            SessionInfo = new SessionInfoPacket
            {
                Identifier = sessionInfo.Identifier,
                Type = sessionInfo.Type,
                Version = sessionInfo.Version,
                Details = { sessionInfo.Details },
                AssociateSessionKeys = { sessionInfo.AssociateSessionKeys }
            }
        };
        foreach (var stream in this.streams)
        {
            var packet = new Packet
            {
                Type = "NewSession",
                SessionKey = sessionInfo.SessionKey,
                IsEssential = false,
                Content = newSessionPacket.ToByteString(),
                Id = this.packetIdGenerator.GetPacketId(),
            };
            this.CreateAndSendPacket(packet, sessionInfo.DataSource, sessionInfo.SessionKey, stream);
        }
    }

    // Sends an EndOfSessionPacket to every stream, then ends the session itself.
    private bool EndSession(string dataSource, string sessionKey)
    {
        
        var endOfSessionPacket = new EndOfSessionPacket
        {
            DataSource = dataSource
        };
        foreach (var stream in this.streams)
        {
            var packet = new Packet
            {
                Type = "EndOfSession",
                SessionKey = sessionKey,
                IsEssential = false,
                Content = endOfSessionPacket.ToByteString(),
                Id = this.packetIdGenerator.GetPacketId(),
            };
            this.CreateAndSendPacket(packet, dataSource, sessionKey, stream);
        }
        var result = this.sessionManagementService.EndSession(dataSource, sessionKey);
        return result.Success;
    }
}