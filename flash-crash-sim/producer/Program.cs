using System.Text;
using System.Text.Json;
using RabbitMQ.Client;

namespace FlashCrashSim.Producer;

internal record Tick(
    string ts,
    string symbol,
    string side,
    decimal price,
    int size,
    string order_type,
    string order_id
);

internal static class Program
{
    private const string ExchangeName = "ticks.exchange";
    private const string RoutingKey = "ticks";
    private const string Symbol = "ACME";

    public static async Task Main(string[] args)
    {
        // Config from env with sensible defaults
        var host = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost";
        var ratePerSec = int.Parse(Environment.GetEnvironmentVariable("TICK_RATE") ?? "50");
        var durationSec = int.Parse(Environment.GetEnvironmentVariable("DURATION_SEC") ?? "0"); // 0 = forever

        Console.WriteLine($"Producer starting: host={host}, rate={ratePerSec}/s, duration={(durationSec == 0 ? "forever" : durationSec + "s")}");

        var factory = new ConnectionFactory { HostName = host, DispatchConsumersAsync = false };
        using var conn = factory.CreateConnection();
        using var channel = conn.CreateModel();

        channel.ExchangeDeclare(ExchangeName, ExchangeType.Direct, durable: true);
        channel.QueueDeclare(queue: "ticks.queue", durable: true, exclusive: false, autoDelete: false);
        channel.QueueBind("ticks.queue", ExchangeName, RoutingKey);

        var rng = new Random(42);
        decimal price = 100.00m;
        long orderSeq = 0;
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var tickInterval = TimeSpan.FromMilliseconds(1000.0 / ratePerSec);
        long sent = 0;

        var props = channel.CreateBasicProperties();
        props.ContentType = "application/json";
        props.DeliveryMode = 2; // persistent

        while (durationSec == 0 || sw.Elapsed.TotalSeconds < durationSec)
        {
            // Random walk with small drift
            var delta = (decimal)((rng.NextDouble() - 0.5) * 0.10);
            price = Math.Max(1m, Math.Round(price + delta, 4));

            var side = rng.NextDouble() < 0.5 ? "BUY" : "SELL";
            var orderType = rng.NextDouble() < 0.85 ? "LIMIT" : (rng.NextDouble() < 0.5 ? "MARKET" : "CANCEL");
            var size = rng.Next(1, 500);

            var tick = new Tick(
                ts: DateTime.UtcNow.ToString("o"),
                symbol: Symbol,
                side: side,
                price: price,
                size: size,
                order_type: orderType,
                order_id: $"O{++orderSeq:D10}"
            );

            var json = JsonSerializer.Serialize(tick);
            var body = Encoding.UTF8.GetBytes(json);
            channel.BasicPublish(ExchangeName, RoutingKey, props, body);
            sent++;

            if (sent % 500 == 0)
                Console.WriteLine($"sent={sent} price={price}");

            await Task.Delay(tickInterval);
        }

        Console.WriteLine($"Done. Total sent={sent}");
    }
}
