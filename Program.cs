// See https://aka.ms/new-console-template for more information

using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using LiveFeedFromFinnhub;

Console.WriteLine("Hello, World!");

var source = new CancellationTokenSource();
var consumerTask = Task.Run(async () =>
{
    
    using var socket = new ClientWebSocket();
    await socket.ConnectAsync(new Uri($"wss://ws.finnhub.io?token={Constants.token}"), source.Token);

    var buffer = new byte[1024];

    var segment = new ArraySegment<byte>(Encoding.UTF8.GetBytes("{\"type\":\"subscribe\",\"symbol\":\"AAPL\"}"));
    await socket.SendAsync(segment, WebSocketMessageType.Text, WebSocketMessageFlags.EndOfMessage, source.Token);

    while (socket.State == WebSocketState.Open)
    {
        var result = await socket.ReceiveAsync(new ArraySegment<byte>(buffer), source.Token);

        if (result.MessageType == WebSocketMessageType.Close)
        {
            await socket.CloseAsync(WebSocketCloseStatus.Empty, string.Empty, source.Token);
        }
        else if (result.MessageType == WebSocketMessageType.Text)
        {
            var str = Encoding.UTF8.GetString(buffer.Take(result.Count).ToArray());
            var msg = JsonSerializer.Deserialize<TradeMessage>(str);
            Console.WriteLine(msg.Trades.Max(x=>x.Price));
        }

    }
});

await consumerTask;