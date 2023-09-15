using System; 
using Confluent.Kafka;

namespace KafkaDemo
{
    // Producer Interface
    public interface IMessageProducer
    {
        void ProduceMessages(string brokerList, string topic);
    }

    // Consumer Interface
    public interface IMessageConsumer
    {
        void ConsumeMessages(string brokerList, string topic);
    }

    // Concrete implementation of IMessageProducer
    public class KafkaMessageProducer : IMessageProducer
    {
        public void ProduceMessages(string brokerList, string topic)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = brokerList,
            };

            var producer = new ProducerBuilder<string, string>(config).Build();
            {
                for (int i = 1; i <= 5; i++)
                {
                    var message = new Message<string, string> { Key = string.Empty, Value = $"Message {i}" };
                    producer.Produce(topic, message, dr => Console.WriteLine($"Produced: {dr.Message.Value}"));
                    producer.Flush(TimeSpan.FromSeconds(10));
                }
            }


        }
    }
  
    // Concrete Implementation of ImessageConsumer
    public class KafkaMessageConsumer : IMessageConsumer
    {
        public void ConsumeMessages(string brokerList, string topic)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = brokerList,
                GroupId = "test-group"
            };
            var consumer = new ConsumerBuilder<string, string>(config).Build();
            {
                consumer.Subscribe(topic);

                while (true)
                {
                    var message = consumer.Consume(TimeSpan.FromSeconds(1));
                    if (message == null)
                        continue;

                    Console.WriteLine($"Consumed: {message.Message.Value}");
                }
            }
        }
    }

    // Message Factory Interface
    public interface IMessageFactory
    {
        IMessageProducer CreateProducer();
        IMessageConsumer CreateConsumer();
    }

    // Concrete Message Factory
    public class KafkaMessageFactory : IMessageFactory
    {
        public IMessageProducer CreateProducer()
        {
            return new KafkaMessageProducer();
        }

        public IMessageConsumer CreateConsumer()
        {
            return new KafkaMessageConsumer();
        }
    }
    class Program
    {
        static void Main(string[] args)
        {
            var brokerList = "localhost:9092";
            var topic = "test";

            try
            {
                // Create an instance of the KafkaMessageFactory
                IMessageFactory factory = new KafkaMessageFactory();

                // Use the factory to create instances of IMessageProducer and IMessageConsumer
                IMessageProducer producer = factory.CreateProducer();
                IMessageConsumer consumer = factory.CreateConsumer();

                // produce and consume messages using producer and consumer
                producer.ProduceMessages(brokerList, topic);
                consumer.ConsumeMessages(brokerList, topic);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred: {ex.Message}");
            }
        }
    }
}


