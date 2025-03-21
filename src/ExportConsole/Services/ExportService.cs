using Confluent.Kafka;
using ExportConsole.Models;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;

namespace ExportConsole.Services
{
    public class ExportService
    {
        private readonly IMongoDbService _mongoDbService;
        private readonly IKafkaProducerService _kafkaProducerService;

        public ExportService(IMongoDbService mongoDbService, IKafkaProducerService kafkaProducerService)
        {
            _mongoDbService = mongoDbService;
            _kafkaProducerService = kafkaProducerService;
        }

        public async Task<ExportResult> RunExportAsync(ExportConfiguration config)
        {
            var database = await _mongoDbService.ConnectToDatabase(config.MongoUrl);

            using var producer = _kafkaProducerService.CreateProducer(config.KafkaBrokers, config.KafkaClientId, config.BatchSize);
            var collection = _mongoDbService.GetCollection(database, config.MongoCollection);

            // Track metrics
            int totalProcessed = 0;
            var startTime = DateTime.UtcNow;

            try
            {
                Console.WriteLine($"Starting batch export with batch size: {config.BatchSize}");

                // Get all documents
                using (var cursor = await _mongoDbService.GetDocumentCursor(collection, config.BatchSize))
                {
                    while (await cursor.MoveNextAsync())
                    {
                        foreach (var document in cursor.Current)
                        {
                            ProcessDocument(document, producer, config.KafkaTopic);
                            totalProcessed++;

                            // Log progress
                            if (totalProcessed % config.BatchSize == 0)
                            {
                                Console.WriteLine($"Processed {totalProcessed} documents");
                            }
                        }
                    }
                    
                    producer.Flush(TimeSpan.FromSeconds(10));
                }

                var duration = DateTime.UtcNow - startTime;
                Console.WriteLine($"Export completed. Processed {totalProcessed} documents");
                Console.WriteLine($"Total time: {duration.TotalSeconds:F2} seconds ({totalProcessed / duration.TotalSeconds:F2} docs/sec)");

                return new ExportResult
                {
                    TotalProcessed = totalProcessed,
                    Duration = duration
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during export: {ex.Message}");
                throw;
            }
        }

        public void ProcessDocument(BsonDocument document, IProducer<string, string> producer, string topic)
        {
            try
            {
                var afidAttr = BsonSerializer.Deserialize<AfidAttributes>(document);
                if (afidAttr != null && !string.IsNullOrEmpty(afidAttr.Afid.ToString()) && afidAttr.Afid != 0)
                {
                    _kafkaProducerService.ProduceMessage(
                        producer,
                        topic,
                        afidAttr.Afid.ToString(),
                        document.ToString());
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing document: {ex.Message}");
            }
        }
    }
}