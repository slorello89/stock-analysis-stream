using System.Text.Json.Serialization;

namespace LiveFeedFromFinnhub;

public class TradeMessage
{
    [JsonPropertyName("data")]
    public TradeDatum[] Trades { get; set; }

    [JsonPropertyName("type")]
    public string Type { get; set; }
}