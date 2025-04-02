using Confluent.Kafka;

namespace ExportConsole.Services
{
    public class KafkaProducerService : IKafkaProducerService
    {
        public IProducer<string, string> CreateProducer(
            string bootstrapServers,
            string clientId,
            int batchSize = 100,
            string? sslKeyPem = null,
            string? sslCertificatePem = null,
            string? sslCaPem = null)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                ClientId = clientId,
                BatchSize = batchSize
            };

            if (!string.IsNullOrEmpty(sslKeyPem) && !string.IsNullOrEmpty(sslCertificatePem) && !string.IsNullOrEmpty(sslCaPem))
            {
                config.SecurityProtocol = SecurityProtocol.Ssl;
                config.SslKeyPem = sslKeyPem;
                config.SslCertificatePem = sslCertificatePem;
                config.SslCaPem = sslCaPem;
            }

            return new ProducerBuilder<string, string>(config).Build();
        }

        public void Flush(IProducer<string, string> producer, TimeSpan timeout) 
            => producer.Flush(timeout);

        public void ProduceMessage(
            IProducer<string, string> producer,
            string topic,
            string key,
            string value)
        {
            producer.Produce(topic, new Message<string, string>
            {
                Key = key,
                Value = value
            });
        }
    }
}