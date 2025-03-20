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
                "localhost",
                "27017",
                "",
                "",
                "defaultdb",
                "test",
                "localhost:9092",
                "test",
                "test",
                100);
#else
            var config = ExportConfiguration.FromEnvironment();
#endif

            await exportService.RunExportAsync(config);
        }
    }
}