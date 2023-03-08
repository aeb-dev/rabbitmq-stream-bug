using System.Buffers;
using System.Net;
using System.Text;
using System.Threading.Channels;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

const int MESSAGE_COUNT = 2000;

var ss = await StreamSystem.Create(
    new()
    {
        Endpoints = {
            new DnsEndPoint("localhost", 5552)
        },
        UserName = "user",
        Password = "password"
    }
);

await ss.CreateStream(new("test"));

bool consumer = true;

if (consumer)
{
    bool working = false;
    if (working)
    {
        await Working();
    }
    else
    {
        await Bugged();
    }
}
else
{
    var p = await Producer.Create(new(ss, "test"));

    const string MESSAGE = """
                            {
                                "x": 2,
                                "y": 3,
                                "u": 0,
                                "obj": {
                                    "x": 1,
                                    "obj": {
                                        "x": 1,
                                        "y": 2
                                    },
                                    "y": 2
                                },
                                "primArr": [
                                    0, 1, 2
                                ],
                                "nPrimArr": [
                                    [0,1,2],
                                    [3,4,5]
                                ],
                                "z": 3
                            }
                            """;

    var bytes = Encoding.UTF8.GetBytes(MESSAGE);

    for (int index = 0; index < MESSAGE_COUNT; ++index)
    {
        await p.Send(new Message(bytes));
    }
}

async Task Bugged()
{
    var ch = Channel.CreateUnbounded<Message>(
        new()
        {
            SingleReader = true,
            SingleWriter = true,
        }
    );

    var c = await Consumer.Create(
        new(ss, "test")
        {
            OffsetSpec = new OffsetTypeFirst(),
            MessageHandler = async (string sourceStream, RawConsumer consumer, MessageContext ctx, Message message) =>
            {
                await ch.Writer.WriteAsync(message);
            }
        }
    );

    await using var fs = new FileStream("test.json", FileMode.Create);
    await using var sw = new StreamWriter(fs);

    await sw.WriteAsync("[");
    await sw.FlushAsync();

    int count = 0;
    await foreach (var m in ch.Reader.ReadAllAsync())
    {
        var json = Encoding.UTF8.GetString(m.Data.Contents.ToArray());
        await sw.WriteAsync(json);
        await sw.WriteAsync(",");
        await sw.FlushAsync();
        ++count;
        if (count == MESSAGE_COUNT) break;
    }

    await sw.WriteAsync("]");
    await sw.FlushAsync();
}

async Task Working()
{
    var ch = Channel.CreateUnbounded<string>(
        new()
        {
            SingleReader = true,
            SingleWriter = true,
        }
    );

    var c = await Consumer.Create(
        new(ss, "test")
        {
            OffsetSpec = new OffsetTypeFirst(),
            MessageHandler = async (string sourceStream, RawConsumer consumer, MessageContext ctx, Message message) =>
            {
                var json = Encoding.UTF8.GetString(message.Data.Contents.ToArray());
                await ch.Writer.WriteAsync(json);
            }
        }
    );

    await using var fs = new FileStream("test.json", FileMode.Create);
    await using var sw = new StreamWriter(fs);

    await sw.WriteAsync("[");

    int count = 0;
    await foreach (var json in ch.Reader.ReadAllAsync())
    {
        await sw.WriteAsync(json);
        await sw.WriteAsync(",");
        await sw.FlushAsync();
        ++count;
        if (count == MESSAGE_COUNT) break;
    }

    await sw.WriteAsync("]");
    await sw.FlushAsync();
}