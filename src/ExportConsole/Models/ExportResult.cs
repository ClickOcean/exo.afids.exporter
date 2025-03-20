namespace ExportConsole.Models
{
    public class ExportResult
    {
        public int TotalProcessed { get; set; }
        public int TotalBatches { get; set; }
        public TimeSpan Duration { get; set; }
    }
}