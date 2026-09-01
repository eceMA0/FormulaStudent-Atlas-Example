using System;

using MA.DataPlatforms.Streaming.Support.Lib.Core.Shared.Abstractions;


namespace WriterCSharp;

internal class Logger : ILogger
{
    private readonly LoggingLevel loggingLevel;

    public Logger(LoggingLevel loggingLevel)
    {
        this.loggingLevel = loggingLevel;
    }

    public void Debug(string message)
    {
        if (this.loggingLevel < LoggingLevel.Debug)
        {
            return;
        }

        Console.WriteLine($"DEBUG: {message}");
    }

    public void Error(string message)
    {
        if (this.loggingLevel < LoggingLevel.Error)
        {
            return;
        }

        Console.WriteLine($"ERROR: {message}");
    }

    public void Info(string message)
    {
        if (this.loggingLevel < LoggingLevel.Info)
        {
            return;
        }

        Console.WriteLine($"INFO: {message}");
    }

    public void Warning(string message)
    {
        if (this.loggingLevel < LoggingLevel.Warning)
        {
            return;
        }

        Console.WriteLine($"WARNING {message}");
    }

    public void Trace(string message)
    {
        if (this.loggingLevel < LoggingLevel.Trace)
        {
            return;
        }

        Console.WriteLine($"TRACE: {message}");
    }
}