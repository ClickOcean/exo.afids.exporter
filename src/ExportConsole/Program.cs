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

            ExportConfiguration config;
            if (args.Length != 0)
            {
                var configFilePath = args[0];
                config = ExportConfiguration.LoadFromJsonFile(configFilePath);
            }
            else
            {
                config = ExportConfiguration.InitFromEnv();
            }

            await exportService.RunExportAsync(config);
        }
    }
}