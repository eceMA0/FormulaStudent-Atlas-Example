namespace WriterCSharp;

internal sealed class ParameterMetadata
{
    public string? Description { get; set; }
    public string? Units { get; set; }
    public string? FormatString { get; set; }
    public double? MinValue { get; set; }
    public double? MaxValue { get; set; }
    public double? WarningMaxValue { get; set; }
}
