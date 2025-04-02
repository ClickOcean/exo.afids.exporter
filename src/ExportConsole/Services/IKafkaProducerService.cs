using Confluent.Kafka;

namespace ExportConsole.Services
{
    public interface IKafkaProducerService
    {
        IProducer<string, string> CreateProducer(
            string bootstrapServers,
            string clientId,
            int batchSize = 100,
            string? sslKeyPem = null,
            string? sslCertificatePem = null,
            string? sslCaPem = null);

        void ProduceMessage(
            IProducer<string, string> producer,
            string topic,
            string key,
            string value);

        void Flush(IProducer<string, string> producer, TimeSpan timeout);
    }
}