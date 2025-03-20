using Confluent.Kafka;

namespace ExportConsole.Services
{
    public class KafkaProducerService : IKafkaProducerService
    {
        public IProducer<string, string> CreateProducer(
            string bootstrapServers,
            string clientId)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                ClientId = clientId
            };
            return new ProducerBuilder<string, string>(config).Build();
        }

        public Task<DeliveryResult<string, string>> ProduceMessageAsync(
            IProducer<string, string> producer,
            string topic,
            string key,
            string value)
        {
            return producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = key,
                Value = value
            });
        }
    }
}