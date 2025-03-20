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
            var database = await _mongoDbService.ConnectToDatabase(config.MongoHost, config.MongoPort, config.MongoUsername, config.MongoPassword, config.MongoDatabase);

            using var producer = _kafkaProducerService.CreateProducer(config.KafkaBrokers, config.KafkaClientId);
            var collection = _mongoDbService.GetCollection(database, config.MongoCollection);

            // Track metrics
            int totalProcessed = 0;
            int totalBatches = 0;
            var startTime = DateTime.UtcNow;

            try
            {
                Console.WriteLine($"Starting batch export with batch size: {config.BatchSize}");

                // Get all documents
                using (var cursor = await _mongoDbService.GetDocumentCursor(collection, config.BatchSize))
                {
                    var batch = new List<BsonDocument>(config.BatchSize);
                    while (await cursor.MoveNextAsync())
                    {
                        foreach (var document in cursor.Current)
                        {
                            batch.Add(document);

                            if (batch.Count >= config.BatchSize)
                            {
                                await ProcessBatch(batch, producer, config.KafkaTopic);
                                totalProcessed += batch.Count;
                                totalBatches++;

                                // Log progress
                                if (totalBatches % 10 == 0)
                                {
                                    Console.WriteLine($"Processed {totalProcessed} documents in {totalBatches} batches");
                                }

                                batch.Clear();
                            }
                        }
                    }

                    // Last docs
                    if (batch.Count > 0)
                    {
                        await ProcessBatch(batch, producer, config.KafkaTopic);
                        totalProcessed += batch.Count;
                        totalBatches++;
                    }
                }

                var duration = DateTime.UtcNow - startTime;
                Console.WriteLine($"Export completed. Processed {totalProcessed} documents in {totalBatches} batches");
                Console.WriteLine($"Total time: {duration.TotalSeconds:F2} seconds ({totalProcessed / duration.TotalSeconds:F2} docs/sec)");

                return new ExportResult
                {
                    TotalProcessed = totalProcessed,
                    TotalBatches = totalBatches,
                    Duration = duration
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during export: {ex.Message}");
                throw;
            }
        }

        public async Task ProcessBatch(List<BsonDocument> batch, IProducer<string, string> producer, string topic)
        {
            var tasks = new List<Task>(batch.Count);
            foreach (var document in batch)
            {
                try
                {
                    var afidAttr = BsonSerializer.Deserialize<AfidAttributes>(document);
                    if (afidAttr != null && !string.IsNullOrEmpty(afidAttr.Afid.ToString()) && afidAttr.Afid != 0)
                    {
                        await _kafkaProducerService.ProduceMessageAsync(
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
}