namespace ExportConsole.Services
{
    public interface IFileService
    {
        Task SaveLastRunDateAsync(DateTime date);
        Task<DateTime?> GetLastRunDateAsync();
        bool EnsureDirectoryExists(string path);
    }
}