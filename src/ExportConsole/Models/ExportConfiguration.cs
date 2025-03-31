using System.Text.Json;
using System.Text.Json.Serialization;

public class ExportConfiguration
{
    [JsonPropertyName("MONGODB_URI")]
    public string MongoUrl { get; }

    [JsonPropertyName("MONGO_COLLECTION")]
    public string MongoCollection { get; }

    [JsonPropertyName("KAFKA_BROKERS")]
    public string KafkaBrokers { get; }

    [JsonPropertyName("KAFKA_CLIENT_ID")]
    public string KafkaClientId { get; }

    [JsonPropertyName("KAFKA_TOPIC")]
    public string KafkaTopic { get; }

    [JsonPropertyName("BATCH_SIZE")]
    public int BatchSize { get; }

    [JsonPropertyName("IS_INITIAL_RUN")]
    public bool IsInitialRun { get; set; } = false;

    public ExportConfiguration()
    {
        MongoUrl = Environment.GetEnvironmentVariable("MONGODB_URI") ?? throw new ArgumentNullException("MONGODB_URI");
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

    public static ExportConfiguration LoadFromJsonFile(string configFilePath)
    {
        if (!File.Exists(configFilePath))
        {
            throw new FileNotFoundException($"Configuration file not found: {configFilePath}");
        }
        string json = File.ReadAllText(configFilePath);
        return JsonSerializer.Deserialize<ExportConfiguration>(json);
    }
}
