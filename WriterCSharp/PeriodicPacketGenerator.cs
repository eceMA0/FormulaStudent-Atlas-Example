using System;
using System.Collections.Generic;
using System.Threading;

using MA.Streaming.OpenData;

namespace WriterCSharp;

// Replays a logged run (see CsvDataSource) as a stream of PeriodicDataPackets.

internal class PeriodicPacketGenerator
{
    private const int SampleCount = 100;

    private readonly CsvDataSource source;
    private readonly ulong interval;
    private readonly ulong dataFormatId;
    private readonly bool realTime;

    private ulong nextTimestamp;
    private int nextRow;

    public PeriodicPacketGenerator(CsvDataSource source, ulong dataFormatId, ulong firstTimestamp, bool realTime = true)
    {
        this.source = source;
        this.dataFormatId = dataFormatId;
        this.nextTimestamp = firstTimestamp;
        this.realTime = realTime;
        this.interval = (ulong)(1e9 / source.Frequency);
    }

    // True once every row in the CSV has been sent - the writer uses this to end the session
    public bool Completed => this.nextRow >= this.source.Rows.Count;

    public PeriodicDataPacket? GeneratePackets()
    {
        if (this.Completed)
        {
            return null;
        }

        var startTime = GenerateCurrentTimestamp();

        var rowCount = Math.Min(SampleCount, this.source.Rows.Count - this.nextRow);
        var parameterCount = this.source.ParameterNames.Length;

        // One sample list per parameter, each holding this block's rows for that channel.
        var columns = new List<DoubleSample>[parameterCount];
        for (var p = 0; p < parameterCount; p++)
        {
            columns[p] = new List<DoubleSample>(rowCount);
        }

        for (var r = 0; r < rowCount; r++)
        {
            var row = this.source.Rows[this.nextRow + r];
            for (var p = 0; p < parameterCount; p++)
            {
                var value = row[p];

                // A gap in the log is published as a Missing sample rather than being dropped, so
                // every column stays the same length and stays aligned with the packet timestamps.
                columns[p].Add(
                    value.HasValue
                        ? new DoubleSample { Value = value.Value, Status = DataStatus.Valid }
                        : new DoubleSample { Value = 0, Status = DataStatus.Missing });
            }
        }

        var packet = new PeriodicDataPacket
        {
            DataFormat = new SampleDataFormat { DataFormatIdentifier = this.dataFormatId },
            StartTime = this.nextTimestamp,
            Interval = (uint)this.interval
        };

        for (var p = 0; p < parameterCount; p++)
        {
            packet.Columns.Add(
                new SampleColumn { DoubleSamples = new DoubleSampleList { Samples = { columns[p] } } });
        }

        this.nextRow += rowCount;
        this.nextTimestamp += this.interval * (ulong)rowCount;

        if (this.realTime)
        {
            // Pace the replay so the packets leave at roughly the rate they were logged at.
            var elapsed = (long)(GenerateCurrentTimestamp() - startTime);
            var sleepNanoseconds = (long)(this.interval * (ulong)rowCount) - elapsed;
            if (sleepNanoseconds > 0)
            {
                Thread.Sleep(TimeSpan.FromTicks(sleepNanoseconds / 100));
            }
        }

        return packet;
    }

    // Generates a timestamp based on UTC epoch, in nanoseconds.
    private static ulong GenerateCurrentTimestamp()
    {
        var ticksSinceEpoch = DateTime.UtcNow.Ticks - DateTime.UnixEpoch.Ticks;
        return (ulong)ticksSinceEpoch * 100;
    }
}
