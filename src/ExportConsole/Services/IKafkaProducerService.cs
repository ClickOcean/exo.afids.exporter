using Confluent.Kafka;

namespace ExportConsole.Services
{
    public interface IKafkaProducerService
    {
        IProducer<string, string> CreateProducer(string bootstrapServers, string clientId);

        Task<DeliveryResult<string, string>> ProduceMessageAsync(
            IProducer<string, string> producer,
            string topic,
            string key,
            string value);
    }
}