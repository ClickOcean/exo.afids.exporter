using System.Text.Json;

namespace ExportConsole.Services
{
    public class FileService : IFileService
    {
        private readonly string _dataDirectory;
        private readonly string _lastRunFile;
        private readonly JsonSerializerOptions options = new() { WriteIndented = true };

        public FileService(string dataDirectory = "/data")
        {
            _dataDirectory = dataDirectory;
            _lastRunFile = Path.Combine(_dataDirectory, "last_run.json");
            EnsureDirectoryExists(_dataDirectory);
        }

        public bool EnsureDirectoryExists(string path)
        {
            if (!Directory.Exists(path))
            {
                try
                {
                    Directory.CreateDirectory(path);
                    return true;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error creating directory {path}: {ex.Message}");
                    return false;
                }
            }
            return true;
        }

        public async Task SaveLastRunDateAsync(DateTime date)
        {
            if (!EnsureDirectoryExists(_dataDirectory))
            {
                throw new DirectoryNotFoundException($"Could not access or create directory: {_dataDirectory}");
            }

            var data = new { LastRunDate = date };
            
            try
            {
                await File.WriteAllTextAsync(_lastRunFile, JsonSerializer.Serialize(data, options));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error saving last run date: {ex.Message}");
                throw;
            }
        }

        public async Task<DateTime?> GetLastRunDateAsync()
        {
            if (!File.Exists(_lastRunFile))
            {
                return null;
            }

            try
            {
                var json = await File.ReadAllTextAsync(_lastRunFile);
                var data = JsonSerializer.Deserialize<LastRunData>(json);
                return data?.LastRunDate;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error reading last run date: {ex.Message}");
                return null;
            }
        }

        private class LastRunData
        {
            public DateTime LastRunDate { get; set; }
        }
    }
}