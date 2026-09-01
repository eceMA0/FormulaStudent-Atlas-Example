using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace WriterCSharp;

// Loads a Life Racing style logged-run CSV so it can be replayed as a historic session.
//
// File layout of the example that the following logic is based on (WriterCSharp/dataFiles/<FILENAME>.csv):
//   line 1: source .LRD path
//   line 2: "0:00.000 to 3:57.558 at 500Hz"   <- sample rate lives here
//   line 3: blank
//   line 4: "Time, Rad_LL_R02, ... , vbat"     <- column header
//   line 5+: one sample row per line
//

//MODIFY if your CSV is imported in a different format, or if you want to support additional quirks in the data.
//
//The following quirks are handled:
//   "####"    - the logger's marker for "no value for this channel at this time"
//   "NEUTRAL", "FIRST", "SECOND", ... - the gear channel is textual rather than numeric
internal sealed class CsvDataSource
{
    private const string MissingValueToken = "####";

    private CsvDataSource(string name, string[] parameterNames, double frequency, IReadOnlyList<double?[]> rows)
    {
        this.Name = name;
        this.ParameterNames = parameterNames;
        this.Frequency = frequency;
        this.Rows = rows;
    }

    // Run name, derived from the file name - used as the session identifier so the replayed
    // run is identifiable in Atlas.
    public string Name { get; }

    // Channel names in column order, excluding the leading Time column.
    public string[] ParameterNames { get; }

    // Samples per second, parsed from the "at 500Hz" header line.
    public double Frequency { get; }

    // One entry per sample row. null in a slot means the channel had no value ("####").
    public IReadOnlyList<double?[]> Rows { get; }

    public static CsvDataSource Load(string path)
    {
        var lines = File.ReadAllLines(path);

        var headerIndex = Array.FindIndex(lines, l => l.TrimStart().StartsWith("Time", StringComparison.OrdinalIgnoreCase));
        if (headerIndex < 0)
        {
            throw new InvalidDataException($"Could not find the 'Time, ...' column header line in '{path}'.");
        }

        // Column 0 is Time, which becomes the packet timestamps rather than a parameter.
        var parameterNames = SplitRow(lines[headerIndex]).Skip(1).ToArray();
        if (parameterNames.Length == 0)
        {
            throw new InvalidDataException($"No parameter columns found in '{path}'.");
        }

        var frequency = ParseFrequency(lines, headerIndex);

        var rows = new List<double?[]>(lines.Length - headerIndex);
        for (var i = headerIndex + 1; i < lines.Length; i++)
        {
            if (string.IsNullOrWhiteSpace(lines[i]))
            {
                continue;
            }

            var fields = SplitRow(lines[i]);
            if (fields.Length < parameterNames.Length + 1)
            {
                // Truncated trailing line - the logger can be cut off mid-row.
                continue;
            }

            var values = new double?[parameterNames.Length];
            for (var c = 0; c < parameterNames.Length; c++)
            {
                values[c] = ParseValue(fields[c + 1]);
            }

            rows.Add(values);
        }

        if (rows.Count == 0)
        {
            throw new InvalidDataException($"No data rows found in '{path}'.");
        }

        return new CsvDataSource(Path.GetFileNameWithoutExtension(path).Trim(), parameterNames, frequency, rows);
    }

    private static string[] SplitRow(string line)
    {
        return line.Split(',').Select(f => f.Trim()).ToArray();
    }

    // The rate line sits between the file header and the column header, e.g. "... at 500Hz".
    private static double ParseFrequency(string[] lines, int headerIndex)
    {
        for (var i = 0; i < headerIndex; i++)
        {
            var match = Regex.Match(lines[i], @"at\s*([0-9]+(?:\.[0-9]+)?)\s*Hz", RegexOptions.IgnoreCase);
            if (match.Success &&
                double.TryParse(match.Groups[1].Value, NumberStyles.Float, CultureInfo.InvariantCulture, out var frequency) &&
                frequency > 0)
            {
                return frequency;
            }
        }

        throw new InvalidDataException("Could not determine the sample rate ('at <n>Hz') from the CSV header.");
    }

    private static double? ParseValue(string field)
    {
        if (field.Length == 0 || field.StartsWith(MissingValueToken, StringComparison.Ordinal))
        {
            return null;
        }

        if (double.TryParse(field, NumberStyles.Float, CultureInfo.InvariantCulture, out var value))
        {
            return value;
        }

        // The gear channel reports textual values ("NEUTRAL", "FIRST", "SECOND", ...) instead
        // of a number; everything else that is non-numeric is treated as no value so a single
        // odd cell cannot abort the replay.
        if (GearNames.TryGetValue(field, out var gear))
        {
            return gear;
        }

        return null;
    }

    private static readonly Dictionary<string, double> GearNames = new(StringComparer.OrdinalIgnoreCase)
    {
        ["NEUTRAL"] = 0,
        ["FIRST"] = 1,
        ["SECOND"] = 2,
        ["THIRD"] = 3,
        ["FOURTH"] = 4,
        ["FIFTH"] = 5,
        ["SIXTH"] = 6,
    };
}
