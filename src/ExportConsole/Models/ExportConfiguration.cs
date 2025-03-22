public class ExportConfiguration
{
    public string MongoUrl { get; }
    public string MongoCollection { get; }
    public string KafkaBrokers { get; }
    public string KafkaClientId { get; }
    public string KafkaTopic { get; }
    public int BatchSize { get; }

    public ExportConfiguration()
    {
        MongoUrl = Environment.GetEnvironmentVariable("MONGO_URI") ?? throw new ArgumentNullException("MONGO_URI");
        MongoCollection = Environment.GetEnvironmentVariable("MONGO_COLLECTION") ?? "afids"; //// Default to 'afids' collection from https://github.com/ClickOcean/difa-api/blob/b5b5773fd5fd48bd07ffe541648ad37141ecd218/src/db/afids/afids.schema.ts#L21
        KafkaBrokers = Environment.GetEnvironmentVariable("KAFKA_BROKERS") ?? throw new ArgumentNullException("KAFKA_BROKERS");
        KafkaClientId = Environment.GetEnvironmentVariable("KAFKA_CLIENT_ID") ?? throw new ArgumentNullException("KAFKA_CLIENT_ID");
        KafkaTopic = Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? throw new ArgumentNullException("KAFKA_TOPIC");
        string batchSizeStr = Environment.GetEnvironmentVariable("BATCH_SIZE") ?? throw new ArgumentNullException("BATCH_SIZE");
        if (!int.TryParse(batchSizeStr, out int parsedBatchSize))
        {
            throw new FormatException($"BATCH_SIZE environment variable '{batchSizeStr}' is not a valid integer");
        }
        BatchSize = parsedBatchSize;
    }

    public ExportConfiguration(string mongoUrl, string mongoCollection, string kafkaBrokers, string kafkaClientId, string kafkaTopic, int batchSize)
    {
        MongoUrl = mongoUrl;
        MongoCollection = mongoCollection;
        KafkaBrokers = kafkaBrokers;
        KafkaClientId = kafkaClientId;
        KafkaTopic = kafkaTopic;
        BatchSize = batchSize;
    }
}
