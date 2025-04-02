using System.Text.Json;
using System.Text.Json.Serialization;

public class ExportConfiguration
{
    private const int DefaultRangeOfDate = 7 * 24;

    [JsonPropertyName("MONGODB_URI")]
    public string MongoUrl { get; init; }

    [JsonPropertyName("MONGO_COLLECTION")]
    public string MongoCollection { get; init; }

    [JsonPropertyName("KAFKA_BROKERS")]
    public string KafkaBrokers { get; init; }

    [JsonPropertyName("KAFKA_CLIENT_ID")]
    public string KafkaClientId { get; init; }

    [JsonPropertyName("KAFKA_TOPIC")]
    public string KafkaTopic { get; init; }

    [JsonPropertyName("BATCH_SIZE")]
    public int BatchSize { get; init; }

    [JsonPropertyName("IS_INITIAL_RUN")]
    public bool IsInitialRun { get; init; } = false;

    [JsonPropertyName("RANGE_IN_HOURS")]
    public int RangeInHours { get; init; } = DefaultRangeOfDate;

    [JsonPropertyName("KAFKA_SSL_KEY_PEM")]
    public string? KafkaSslKeyPem { get; set; }

    [JsonPropertyName("KAFKA_SSL_CERTIFICATE_PEM")]
    public string? KafkaSslCertificatePem { get; set; }

    [JsonPropertyName("KAFKA_SSL_CA_PEM")]
    public string? KafkaSslCaPem { get; set; }

    public ExportConfiguration()
    {
    }

    public static ExportConfiguration InitFromEnv()
    {
        return new ExportConfiguration()
        {
            MongoUrl = Environment.GetEnvironmentVariable("MONGODB_URI") ?? throw new ArgumentNullException("MONGODB_URI"),
            MongoCollection = Environment.GetEnvironmentVariable("MONGO_COLLECTION") ?? "afids", //// Default to 'afids' collection from https://github.com/ClickOcean/difa-api/blob/b5b5773fd5fd48bd07ffe541648ad37141ecd218/src/db/afids/afids.schema.ts#L21
            KafkaBrokers = Environment.GetEnvironmentVariable("KAFKA_BROKERS") ?? throw new ArgumentNullException("KAFKA_BROKERS"),
            KafkaClientId = Environment.GetEnvironmentVariable("KAFKA_CLIENT_ID") ?? throw new ArgumentNullException("KAFKA_CLIENT_ID"),
            KafkaTopic = Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? throw new ArgumentNullException("KAFKA_TOPIC"),
            RangeInHours = int.TryParse(Environment.GetEnvironmentVariable("RANGE_IN_HOURS"), out int parsedRangeInHours) ? parsedRangeInHours : DefaultRangeOfDate,
            BatchSize = !int.TryParse(Environment.GetEnvironmentVariable("BATCH_SIZE") ?? throw new ArgumentNullException("BATCH_SIZE"), out int parsedBatchSize)
            ? throw new FormatException($"BATCH_SIZE environment variable is not a valid integer")
            : parsedBatchSize,
            IsInitialRun = bool.TryParse(Environment.GetEnvironmentVariable("IS_INITIAL_RUN"), out bool parsedIsInitialRun) && parsedIsInitialRun,
            KafkaSslCaPem = Environment.GetEnvironmentVariable("KAFKA_SSL_CA_PEM"),
            KafkaSslCertificatePem = Environment.GetEnvironmentVariable("KAFKA_SSL_CERTIFICATE_PEM"),
            KafkaSslKeyPem = Environment.GetEnvironmentVariable("KAFKA_SSL_KEY_PEM"),
        };
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
