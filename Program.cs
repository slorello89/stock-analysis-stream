// See https://aka.ms/new-console-template for more information

using System.ComponentModel.DataAnnotations;
using System.Net.WebSockets;
using System.Reflection.Emit;
using System.Text;
using System.Text.Json;
using LiveFeedFromFinnhub;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;
using NRedisTimeSeries;
using NRedisTimeSeries.Commands.Enums;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;

Console.WriteLine("Hello, World!");

var muxer = await ConnectionMultiplexer.ConnectAsync("localhost,$SELECT=");

var db = muxer.GetDatabase();

var builder = new ConfigurationBuilder();

builder.Add(new JsonConfigurationSource{Path = "appsettings.json"});

var config = builder.Build();

var source = new CancellationTokenSource();
var tasks = new List<Task>();
var securities = new[] {"DIS", "AAPL", "TWTR", "MSFT", "TSLA"};
var timeWindows = new[] {5, 10, 20};

foreach (var security in securities)
{
    var priceRootName = $"ts:{security}:price";
    var volumeRootName = $"ts:{security}:volume";
    
    tasks.Add(db.TimeSeriesCreateAsync(priceRootName, 
        labels: new TimeSeriesLabel[]
        {
            new TimeSeriesLabel("type","raw"),
            new TimeSeriesLabel("security", security),
            new TimeSeriesLabel("component", "price")
        }));
    tasks.Add(db.TimeSeriesCreateAsync(volumeRootName,
        labels: new TimeSeriesLabel[]
        {
            new TimeSeriesLabel("type", "raw"),
            new TimeSeriesLabel("security", security),
            new TimeSeriesLabel("component","volume")
        }));
    foreach (var window in timeWindows)
    {
        tasks.Add(db.TimeSeriesCreateAsync($"{priceRootName}:avg:{window}",
            labels: new []
            {
                new TimeSeriesLabel("type","aggregation"),
                new TimeSeriesLabel("security", security),
                new TimeSeriesLabel("component", "price")
            }));
        tasks.Add(db.TimeSeriesCreateRuleAsync(priceRootName, new TimeSeriesRule($"{priceRootName}:avg:{window}", window * 1000, TsAggregation.Avg)));
        
        tasks.Add(db.TimeSeriesCreateAsync($"{priceRootName}:std:{window}",
            labels: new []
            {
                new TimeSeriesLabel("type","aggregation"),
                new TimeSeriesLabel("security", security),
                new TimeSeriesLabel("component", "price")
            }));
        tasks.Add(db.TimeSeriesCreateRuleAsync(priceRootName, new TimeSeriesRule($"{priceRootName}:std:{window}", window * 1000, TsAggregation.StdP)));
        
        tasks.Add(db.TimeSeriesCreateAsync($"{volumeRootName}:avg:{window}",
            labels: new []
            {
                new TimeSeriesLabel("type","aggregation"),
                new TimeSeriesLabel("security", security),
                new TimeSeriesLabel("component", "volume")
            }));
        tasks.Add(db.TimeSeriesCreateRuleAsync(volumeRootName, new TimeSeriesRule($"{volumeRootName}:avg:{window}", window * 1000, TsAggregation.Avg)));
        
        tasks.Add(db.TimeSeriesCreateAsync($"{volumeRootName}:std:{window}",
            labels: new []
            {
                new TimeSeriesLabel("type","aggregation"),
                new TimeSeriesLabel("security", security),
                new TimeSeriesLabel("component", "volume")
            }));
        tasks.Add(db.TimeSeriesCreateRuleAsync(volumeRootName, new TimeSeriesRule($"{volumeRootName}:std:{window}", window * 1000, TsAggregation.StdP)));
        
        tasks.Add(db.TimeSeriesCreateAsync($"{volumeRootName}:sum:{window}",
            labels: new []
            {
                new TimeSeriesLabel("type","aggregation"),
                new TimeSeriesLabel("security", security),
                new TimeSeriesLabel("component", "volume")
            }));
        tasks.Add(db.TimeSeriesCreateRuleAsync(volumeRootName, new TimeSeriesRule($"{volumeRootName}:sum:{window}", window * 1000, TsAggregation.Sum)));
    }
}

await Task.WhenAll(tasks);
tasks.Clear();



var thread = new Thread(async () =>
{
    
    using var socket = new ClientWebSocket();
    await socket.ConnectAsync(new Uri($"wss://ws.finnhub.io?token={config["token"]}"), source.Token);

    var buffer = new byte[1024];

    foreach (var security in securities)
    {
        var segment = new ArraySegment<byte>(Encoding.UTF8.GetBytes($"{{\"type\":\"subscribe\",\"symbol\":\"{security}\"}}"));
        await socket.SendAsync(segment, WebSocketMessageType.Text, WebSocketMessageFlags.EndOfMessage, source.Token);
    }
    

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
            // Console.WriteLine(str);
            var msg = JsonSerializer.Deserialize<TradeMessage>(str);
            if (msg.Type == "trade")
            {
                try
                {
                    foreach (var securityGroup in msg.Trades.GroupBy(x => x.Symbol))
                    {
                        var messageString = $"{securityGroup.Key} - {securityGroup.Max(x => x.Price)}";
                        var messageStringVolume = $"{securityGroup.Key} - {securityGroup.Sum(x => x.Volume)}";
                        
                        tasks.Add(db.TimeSeriesAddAsync($"ts:{securityGroup.Key}:price", new TimeStamp(securityGroup.First().Timestamp), securityGroup.Average(x => x.Price), duplicatePolicy: TsDuplicatePolicy.LAST));
                        tasks.Add(db.TimeSeriesAddAsync($"ts:{securityGroup.Key}:volume", new TimeStamp(securityGroup.First().Timestamp), securityGroup.Sum(x => x.Volume), duplicatePolicy: TsDuplicatePolicy.SUM));
                        // Console.WriteLine(messageString);
                        // Console.WriteLine(messageStringVolume);
                    }

                    await Task.WhenAll(tasks);
                    tasks.Clear();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }
            }
        }
    }
});

thread.Start();


var consumerTask = Task.Run(async () =>
{
    IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, TimeSeriesTuple value)> results;
    while (true)
    {
        results = await db.TimeSeriesMGetAsync(new []{"security=MSFT"});
        if (results.All(x => x.value != null))
        {
            break;
        }
        await Task.Delay(500);
    }

    var timestamp = results.Max(x => (long)x.value.Time);
    
    while (true)
    {
        var next = await db.TimeSeriesMRangeAsync(timestamp, "+", new[] {"security=MSFT"});
        if (next.Any(x => !x.values.Any()))
        {
            await Task.Delay(500);
            continue;
        }

        timestamp = next.Max(x => (long)x.values.Last().Time);

        foreach (var element in next.Select(x=>new {val = x.values.Last(),key = x.key}))
        {
            Console.WriteLine($"{element.key}, {element.val.Val}");
        }
        
        await Task.Delay(50);

    }

});

await Task.WhenAll(consumerTask);

thread.Join();