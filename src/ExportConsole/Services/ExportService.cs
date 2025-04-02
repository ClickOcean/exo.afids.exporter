using Confluent.Kafka;
using ExportConsole.Models;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using System.Text.Json;

namespace ExportConsole.Services
{
    public class ExportService(
        IMongoDbService mongoDbService,
        IKafkaProducerService kafkaProducerService)
    {
        public async Task<ExportResult> RunExportAsync(ExportConfiguration config)
        {
            var database = await mongoDbService.ConnectToDatabase(config.MongoUrl);

            using var producer = kafkaProducerService.CreateProducer(config.KafkaBrokers, config.KafkaClientId, config.BatchSize, config.KafkaSslKeyPem, config.KafkaSslCertificatePem, config.KafkaSslCaPem);
            var collection = mongoDbService.GetCollection(database, config.MongoCollection);

            // Track metrics
            int totalProcessed = 0;
            var startTime = DateTime.UtcNow;

            // Calculate start date based on last run date. Null if initial run for full DB export
            DateTime? startDate = config.IsInitialRun ? null : DateTime.UtcNow.AddHours(-config.RangeInHours);
            try
            {
                Console.WriteLine($"Starting batch export with batch size: {config.BatchSize}");

                // Get documents filtered by date (if lastRunDate is available)
                using (var cursor = await mongoDbService.GetDocumentCursorWithDateFilter(collection, startDate, config.BatchSize))
                {
                    while (await cursor.MoveNextAsync())
                    {
                        foreach (var document in cursor.Current)
                        {
                            ProcessDocument(document, producer, config.KafkaTopic);
                            totalProcessed++;

                            // Log progress every batch
                            if (totalProcessed % config.BatchSize == 0)
                            {
                                Console.WriteLine($"Processed {totalProcessed} documents");
                                producer.Flush(CancellationToken.None);
                            }
                        }
                    }
                }

                producer.Flush(CancellationToken.None);
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
                    // Use Afid as the unique key for each document
                    var key = afidAttr.Afid.ToString();
                    kafkaProducerService.ProduceMessage(
                        producer,
                        topic,
                        key,
                        JsonSerializer.Serialize(afidAttr));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing document: {ex.Message}");
            }
        }
    }
}