using ExportConsole.Services;

namespace ExportConsole
{
    public class Program
    {
        static async Task Main(string[] args)
        {
            var mongoDbService = new MongoDbService();
            var kafkaProducerService = new KafkaProducerService();
            var exportService = new ExportService(mongoDbService, kafkaProducerService);

#if DEBUG
            var config = new ExportConfiguration(
                "mongodb://localhost:27017/defaultdb",
                "test",
                "localhost:9092",
                "test",
                "default-topic",
                100)
            {
                IsInitialRun = true
            };
#else
            var config = new ExportConfiguration();

            if (args.Length != 0)
            {
                var configFilePath = args[0];
                config = ExportConfiguration.LoadFromJsonFile(configFilePath);
            }
#endif

            await exportService.RunExportAsync(config);
        }
    }
}