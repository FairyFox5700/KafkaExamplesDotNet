using System.Text.Json;

using Confluent.Kafka;

namespace WikiEditStreamWithKafka.Producers
{
    public class WikiStreamProducer : IHostedService
    {
        private readonly ILogger<WikiStreamProducer> _logger;
        private readonly string _eventStreamsUrl = "https://stream.wikimedia.org/v2/stream/recentchange";
        private readonly IProducer<string?, string> _producer;
        private string _topicName = "wiki-stream";
        public WikiStreamProducer(ILogger<WikiStreamProducer> logger)
        {
            _logger = logger;
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9091",
            };
            _producer = new ProducerBuilder<string?, string>(config).Build();
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation($"{nameof(WikiStreamProducer)} starting...");
            try
            {
                using (var httpClient = new HttpClient())
                using (var stream = await httpClient.GetStreamAsync(_eventStreamsUrl))
                using (var reader = new StreamReader(stream))
                {
                    while (!reader.EndOfStream)
                    {
                        var line = await reader.ReadLineAsync();
                        if (line != null && !line.StartsWith("data:"))
                        {
                            continue;
                        }

                        if (line != null)
                        {
                            int openBraceIndex = line.IndexOf('{');
                            string jsonData = line.Substring(openBraceIndex);
                            _logger.LogInformation($"Data string: {jsonData}");

                            var jsonDoc = JsonDocument.Parse(jsonData);
                            var metaElement = jsonDoc.RootElement.GetProperty("meta");
                            var uriElement = metaElement.GetProperty("uri");
                            var key = uriElement.GetString();

                            _producer.Produce(_topicName, new Message<string?, string> { Key = key, Value = jsonData },
                                (deliveryReport) =>
                                {
                                    if (deliveryReport.Error.Code != ErrorCode.NoError)
                                    {
                                        _logger.LogWarning($"Failed to deliver message: {deliveryReport.Error.Reason}");
                                    }
                                    else
                                    {
                                        _logger.LogWarning($"Produced message to: {deliveryReport.TopicPartitionOffset}");
                                    }
                                });
                        }
                    }
                }
            }
            finally

            {
                var queueSize = _producer.Flush(TimeSpan.FromSeconds(5));
                if (queueSize > 0)
                {
                    _logger.LogWarning("WARNING: Producer event queue has " + queueSize +
                                           " pending events on exit.");
                }
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _producer?.Dispose();
            return Task.CompletedTask;
        }
    }
}
