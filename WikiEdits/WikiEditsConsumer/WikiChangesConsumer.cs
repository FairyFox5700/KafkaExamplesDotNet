using System.Text.Json;

using Confluent.Kafka;

namespace WikiEditsConsumer
{
    public class WikiChangesConsumer : IHostedService
    {
        private readonly ILogger<WikiChangesConsumer> _logger;
        private readonly ConsumerConfig _config;
        private readonly string groupId = "wiki-edit-stream-group-1";
        private readonly string bootstrapServers = "localhost:9091";
        private string _topicName = "wiki-stream";

        public WikiChangesConsumer(ILogger<WikiChangesConsumer> logger)
        {
            _logger = logger;


            _config = new ConsumerConfig
            {
                GroupId = groupId,
                BootstrapServers = bootstrapServers,
                // The offset to start reading from if there are no committed offsets (or there was an error in retrieving offsets).
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
            };
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation($"{nameof(WikiChangesConsumer)} starting");

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            // Build a consumer that uses the provided configuration.
            using (var consumer = new ConsumerBuilder<string, string>(_config).Build())
            {
                // Subscribe to events from the topic.
                consumer.Subscribe(_topicName);
                try
                {
                    // Run until the terminal receives Ctrl+C.
                    while (true)
                    {
                        // Consume and deserialize the next message.
                        var cr = consumer.Consume(cts.Token);
                        // Parse the JSON to extract the URI of the edited page.
                        var jsonDoc = JsonDocument.Parse(cr.Message.Value);
                        // For consuming from the recent_changes topic.
                        var metaElement = jsonDoc.RootElement.GetProperty("meta");
                        var uriElement = metaElement.GetProperty("uri");
                        var uri = uriElement.GetString();
                        _logger.LogInformation($"Consumed record with URI {uri}");
                    }
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation($"Ctrl+C pressed, consumer exiting");
                }
                finally
                {
                    consumer.Close();
                }

                return Task.CompletedTask;
            }
        }


        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
