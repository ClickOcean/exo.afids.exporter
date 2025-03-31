using ExportConsole.Services;

namespace ExportConsole
{
    public class Program
    {
        static async Task Main(string[] args)
        {
            var mongoDbService = new MongoDbService();
            var kafkaProducerService = new KafkaProducerService();
            var fileService = new FileService();
            var exportService = new ExportService(mongoDbService, kafkaProducerService, fileService);

#if DEBUG
            var config = new ExportConfiguration(
                "mongodb://localhost:27017/defaultdb",
                "test",
                "localhost:9092",
                "test",
                "default-topic",
                100);
#else
            var config = new ExportConfiguration();
#endif

            await exportService.RunExportAsync(config);
        }
    }
}