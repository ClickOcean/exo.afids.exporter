public class ExportConfiguration
{
    public string MongoHost { get; }
    public string MongoPort { get; }
    public string MongoUsername { get; }
    public string MongoPassword { get; }
    public string MongoDatabase { get; }
    public string KafkaBrokers { get; }
    public string KafkaClientId { get; }
    public string KafkaTopic { get; }
    public string MongoCollection { get; }
    public int BatchSize { get; }

    public ExportConfiguration()
    {
        MongoHost = Environment.GetEnvironmentVariable("MONGO_HOST") ?? throw new ArgumentNullException("MONGO_HOST");
        MongoPort = Environment.GetEnvironmentVariable("MONGO_PORT") ?? throw new ArgumentNullException("MONGO_PORT");
        MongoUsername = Environment.GetEnvironmentVariable("MONGO_USERNAME") ?? throw new ArgumentNullException("MONGO_USERNAME");
        MongoPassword = Environment.GetEnvironmentVariable("MONGO_PASSWORD") ?? throw new ArgumentNullException("MONGO_PASSWORD");
        MongoDatabase = Environment.GetEnvironmentVariable("MONGO_DATABASE") ?? throw new ArgumentNullException("MONGO_DATABASE");
        KafkaBrokers = Environment.GetEnvironmentVariable("KAFKA_BROKERS") ?? throw new ArgumentNullException("KAFKA_BROKERS");
        KafkaClientId = Environment.GetEnvironmentVariable("KAFKA_CLIENT_ID") ?? throw new ArgumentNullException("KAFKA_CLIENT_ID");
        KafkaTopic = Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? throw new ArgumentNullException("KAFKA_TOPIC");
        MongoCollection = Environment.GetEnvironmentVariable("MONGO_COLLECTION") ?? throw new ArgumentNullException("MONGO_COLLECTION");
        string batchSizeStr = Environment.GetEnvironmentVariable("BATCH_SIZE") ?? throw new ArgumentNullException("BATCH_SIZE");
        if (!int.TryParse(batchSizeStr, out int parsedBatchSize))
        {
            throw new FormatException($"BATCH_SIZE environment variable '{batchSizeStr}' is not a valid integer");
        }
        BatchSize = parsedBatchSize;
    }

    public ExportConfiguration(string mongoHost, string mongoPort, string mongoUsername, string mongoPassword, string mongoDatabase, string kafkaBrokers, string kafkaClientId, string kafkaTopic, string mongoCollection, int batchSize)
    {
        MongoHost = mongoHost;
        MongoPort = mongoPort;
        MongoUsername = mongoUsername;
        MongoPassword = mongoPassword;
        MongoDatabase = mongoDatabase;
        KafkaBrokers = kafkaBrokers;
        KafkaClientId = kafkaClientId;
        KafkaTopic = kafkaTopic;
        MongoCollection = mongoCollection;
        BatchSize = batchSize;
    }
}
