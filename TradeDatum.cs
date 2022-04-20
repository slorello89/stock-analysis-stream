using System.Text.Json.Serialization;

namespace LiveFeedFromFinnhub;

public class TradeDatum
{
    [JsonPropertyName("p")]
    public double Price { get; set; }

    [JsonPropertyName("s")]
    public string Symbol { get; set; }

    [JsonPropertyName("v")]
    public int Volume { get; set; }
    
    [JsonPropertyName("t")]
    public long Timestamp { get; set; }
}