﻿using Confluent.Kafka;
using ExportConsole.Models;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using System.Text.Json;

namespace ExportConsole.Services
{
    public class ExportService
    {
        private readonly IMongoDbService _mongoDbService;
        private readonly IKafkaProducerService _kafkaProducerService;
        private readonly IFileService _fileService;

        public ExportService(
            IMongoDbService mongoDbService, 
            IKafkaProducerService kafkaProducerService,
            IFileService fileService)
        {
            _mongoDbService = mongoDbService;
            _kafkaProducerService = kafkaProducerService;
            _fileService = fileService;
        }

        public async Task<ExportResult> RunExportAsync(ExportConfiguration config)
        {
            var database = await _mongoDbService.ConnectToDatabase(config.MongoUrl);

            using var producer = _kafkaProducerService.CreateProducer(config.KafkaBrokers, config.KafkaClientId, config.BatchSize);
            var collection = _mongoDbService.GetCollection(database, config.MongoCollection);

            // Track metrics
            int totalProcessed = 0;
            var startTime = DateTime.UtcNow;
            
            // Retrieve the last run date if available
            var lastRunDate = await _fileService.GetLastRunDateAsync();
            if (lastRunDate.HasValue)
            {
                Console.WriteLine($"Last export run on: {lastRunDate.Value}");
            }
            else
            {
                Console.WriteLine("This is the first export run.");
            }

            try
            {
                Console.WriteLine($"Starting batch export with batch size: {config.BatchSize}");

                // Get documents filtered by date (if lastRunDate is available)
                using (var cursor = await _mongoDbService.GetDocumentCursorWithDateFilter(collection, lastRunDate, config.BatchSize))
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
                                producer.Flush(CancellationToken.None);
                            }
                        }
                    }                    
                }

                producer.Flush(CancellationToken.None);
                var duration = DateTime.UtcNow - startTime;
                Console.WriteLine($"Export completed. Processed {totalProcessed} documents");
                Console.WriteLine($"Total time: {duration.TotalSeconds:F2} seconds ({totalProcessed / duration.TotalSeconds:F2} docs/sec)");

                // Persist the current run date
                await _fileService.SaveLastRunDateAsync(DateTime.UtcNow);
                Console.WriteLine($"Saved current run date: {DateTime.UtcNow}");

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
                    //// Clear unique key for each document
                    var key = afidAttr.Afid.ToString();
                    _kafkaProducerService.ProduceMessage(
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