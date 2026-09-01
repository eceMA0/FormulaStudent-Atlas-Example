using System;
using System.IO;

using MA.DataPlatforms.Streaming.Support.Lib.Core.Abstractions;
using MA.Streaming.Abstraction;
using MA.Streaming.Core.Configs;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

namespace WriterCSharp;

internal static class Program
{
    // Default location of the logged run, relative to the repo root. Override by passing a path
    // as the first command line argument.
    private const string DefaultCsvRelativePath = @"dataFiles\examplerun.csv"; // EDIT THIS. Path to the CSV file to be read

    private static void Main(string[] args)
    {

        var projectRoot = Directory.GetParent(AppContext.BaseDirectory)!
            .Parent!
            .Parent!
            .Parent!
            .FullName;


        var csvPath = args.Length > 0
            ? args[0]
            : Path.Combine(projectRoot, DefaultCsvRelativePath);

        CsvDataSource csvSource;
        try
        {
            csvSource = CsvDataSource.Load(csvPath);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Failed to load CSV '{csvPath}': {ex.Message}");
            return;
        }

        var metadataPath = Path.ChangeExtension(csvPath, ".metadata.json");

        Dictionary<string, ParameterMetadata> metadata;

        if (File.Exists(metadataPath))
        {
            metadata =
                JsonSerializer.Deserialize<Dictionary<string, ParameterMetadata>>(
                    File.ReadAllText(metadataPath),
                    new JsonSerializerOptions { PropertyNameCaseInsensitive = true })
                ?? new Dictionary<string, ParameterMetadata>();

            Console.WriteLine($"Loaded metadata for {metadata.Count} channel(s) from '{metadataPath}'.");

            foreach (var name in metadata.Keys)
            {
                if (!csvSource.ParameterNames.Contains(name))
                {
                    Console.WriteLine($"  WARNING: metadata entry '{name}' matches no channel in the CSV.");
                }
            }
        }
        else
        {
            metadata = new Dictionary<string, ParameterMetadata>();
            Console.WriteLine($"No metadata file found at '{metadataPath}' - using values derived from the CSV.");
        }

        Console.WriteLine(
            $"Loaded '{csvSource.Name}': {csvSource.Rows.Count} rows, " +
            $"{csvSource.ParameterNames.Length} channels at {csvSource.Frequency}Hz.");

        // Step 2: bootstrap the support library (logger + broker config + service API entry point).
        var logger = new Logger(LoggingLevel.Info);
        var streamApiConfig = new StreamingApiConfiguration(StreamCreationStrategy.TopicBased, "localhost:9094", []);

        var supportLibApi = new SupportLibApiFactory().Create(logger, streamApiConfig);
        supportLibApi.Initialise();
        supportLibApi.Start();

        Console.WriteLine("Support library started.");

        // Step 3: get the 3 modules you need.
        var writingModule = supportLibApi.GetWritingPacketApi();
        if (writingModule is null)
        {
            logger.Error("Writing packet module API is not available.");
            return;
        }

        var writingServiceResult = writingModule.CreateService();
        if (!writingServiceResult.Success || writingServiceResult.Data is null)
        {
            logger.Error($"Failed to create packet writer service: {writingServiceResult.Message}");
            return;
        }

        var packetWriter = writingServiceResult.Data;
        packetWriter.Initialise();
        packetWriter.Start();

        var sessionModule = supportLibApi.GetSessionManagerApi();
        if (sessionModule is null)
        {
            logger.Error("Session manager module API is not available.");
            return;
        }

        var sessionServiceResult = sessionModule.CreateService();
        if (!sessionServiceResult.Success || sessionServiceResult.Data is null)
        {
            logger.Error($"Failed to create session management service: {sessionServiceResult.Message}");
            return;
        }

        var sessionMgmt = sessionServiceResult.Data;
        sessionMgmt.Initialise();
        sessionMgmt.Start();

        var dataFormatModule = supportLibApi.GetDataFormatManagerApi();
        if (dataFormatModule is null)
        {
            logger.Error("Data format manager module API is not available.");
            return;
        }

        var dataFormatServiceResult = dataFormatModule.CreateService();
        if (!dataFormatServiceResult.Success || dataFormatServiceResult.Data is null)
        {
            logger.Error($"Failed to create data format management service: {dataFormatServiceResult.Message}");
            return;
        }

        var dataFormatMgmt = dataFormatServiceResult.Data;
        dataFormatMgmt.Initialise();
        dataFormatMgmt.Start();

        var writer = new MockDataWriter(packetWriter, dataFormatMgmt, sessionMgmt, logger, csvSource, metadata);
        writer.CreateStartWriteAndEndMockSession();

    }       
}