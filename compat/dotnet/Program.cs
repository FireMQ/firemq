// RabbitMQ tutorial scenarios with the RabbitMQ .NET client (7.x) against FireMQ.
// Run: AMQP_URL=amqp://guest:guest@localhost:5672 dotnet run
using System.Collections.Concurrent;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

var url = Environment.GetEnvironmentVariable("AMQP_URL") ?? "amqp://guest:guest@localhost:5672";
var factory = new ConnectionFactory { Uri = new Uri(url) };
await using var conn = await factory.CreateConnectionAsync("firemq-compat-dotnet");
var failures = 0;

string Uid(string p) => $"{p}-{Guid.NewGuid().ToString()[..8]}";
byte[] B(string s) => Encoding.UTF8.GetBytes(s);
void Expect(bool cond, string what) { if (!cond) throw new Exception(what); }

async Task Test(string name, Func<Task> body)
{
    try { await body(); Console.WriteLine($"ok    {name}"); }
    catch (Exception e) { failures++; Console.WriteLine($"FAIL  {name}: {e.Message}"); }
}

await Test("hello world", async () =>
{
    await using var ch = await conn.CreateChannelAsync();
    var q = Uid("hello");
    await ch.QueueDeclareAsync(q, false, false, false);
    await ch.BasicPublishAsync("", q, B("Hello World!"));
    var got = new TaskCompletionSource<string>();
    var consumer = new AsyncEventingBasicConsumer(ch);
    consumer.ReceivedAsync += (_, ea) => { got.TrySetResult(Encoding.UTF8.GetString(ea.Body.Span)); return Task.CompletedTask; };
    await ch.BasicConsumeAsync(q, true, consumer);
    Expect(await got.Task.WaitAsync(TimeSpan.FromSeconds(5)) == "Hello World!", "message received");
});

await Test("work queues with qos and manual ack", async () =>
{
    await using var ch = await conn.CreateChannelAsync();
    var q = Uid("tasks");
    await ch.QueueDeclareAsync(q, true, false, false);
    for (var i = 0; i < 5; i++)
        await ch.BasicPublishAsync("", q, true, new BasicProperties { Persistent = true }, B($"task {i}"));
    await ch.BasicQosAsync(0, 1, false);
    var got = new ConcurrentQueue<string>();
    var done = new TaskCompletionSource();
    var consumer = new AsyncEventingBasicConsumer(ch);
    consumer.ReceivedAsync += async (_, ea) =>
    {
        got.Enqueue(Encoding.UTF8.GetString(ea.Body.Span));
        await ch.BasicAckAsync(ea.DeliveryTag, false);
        if (got.Count == 5) done.TrySetResult();
    };
    await ch.BasicConsumeAsync(q, false, consumer);
    await done.Task.WaitAsync(TimeSpan.FromSeconds(5));
    Expect(got.SequenceEqual(Enumerable.Range(0, 5).Select(i => $"task {i}")), "in order");
});

await Test("topics", async () =>
{
    await using var ch = await conn.CreateChannelAsync();
    var ex = Uid("topic_logs");
    await ch.ExchangeDeclareAsync(ex, ExchangeType.Topic);
    var q = (await ch.QueueDeclareAsync()).QueueName;
    await ch.QueueBindAsync(q, ex, "*.critical");
    await ch.BasicPublishAsync(ex, "kern.critical", B("yes"));
    await ch.BasicPublishAsync(ex, "kern.info", B("no"));
    await Task.Delay(100);
    var r = await ch.BasicGetAsync(q, true);
    Expect(r != null && Encoding.UTF8.GetString(r.Body.Span) == "yes", "routed");
    Expect(await ch.BasicGetAsync(q, true) == null, "nothing else");
});

await Test("publisher confirms", async () =>
{
    var opts = new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);
    await using var ch = await conn.CreateChannelAsync(opts);
    var q = Uid("confirmed");
    await ch.QueueDeclareAsync(q, true, false, false);
    for (var i = 0; i < 50; i++)
        await ch.BasicPublishAsync("", q, true, new BasicProperties { Persistent = true }, B($"m{i}"));
    Expect(await ch.MessageCountAsync(q) == 50, "50 messages");
});

await Test("mandatory unroutable publish is returned", async () =>
{
    var opts = new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);
    await using var ch = await conn.CreateChannelAsync(opts);
    try
    {
        await ch.BasicPublishAsync("", Uid("nowhere"), true, new BasicProperties(), B("lost"));
        throw new Exception("expected a returned publish");
    }
    catch (PublishException e) { Expect(e.IsReturn, "IsReturn"); }
});

await Test("inequivalent redeclare closes only the channel", async () =>
{
    await using var ch = await conn.CreateChannelAsync();
    var q = Uid("strict");
    await ch.QueueDeclareAsync(q, false, false, false);
    try
    {
        await ch.QueueDeclareAsync(q, true, false, false);
        throw new Exception("expected an error");
    }
    catch (OperationInterruptedException e) { Expect(e.ShutdownReason?.ReplyCode == 406, "406"); }
    Expect(conn.IsOpen, "connection still open");
});

Console.WriteLine(failures == 0 ? "all passed" : $"{failures} failed");
return failures == 0 ? 0 : 1;
