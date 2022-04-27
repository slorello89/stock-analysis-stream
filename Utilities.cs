using System.Text;
using StackExchange.Redis;

namespace LiveFeedFromFinnhub;

public static class Utilities
{
    public static readonly char[] EscapeChars = new char[] {
        ',', '.', '<', '>', '{', '}', '[', ']', '"', '\'', ':', ';',
        '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '-', '+', '=', '~', '|', ' ',
    };
    
    
    public static string EscapeForRedis(this string text)
    {
        var sb = new StringBuilder();
        foreach (var c in text)
        {
            if (EscapeChars.Contains(c))
            {
                sb.Append("\\");
            }

            sb.Append(c);
        }

        return sb.ToString();
    }

    public static Dictionary<string, string> ToDictionary(this RedisResult inResult)
    {
        var result = new Dictionary<string, string>();
        var arr = (RedisResult[])inResult;
        for (var i = 0; i < arr.Length; i += 2)
        {
            result.Add((string)arr[i], (string)arr[i+1]);
        }
        
        return result;
    }

    public static List<VectorSimilarityResult> ToVSSResult(this RedisResult inResult)
    {
        var result = new List<VectorSimilarityResult>();
        var arr = (RedisResult[]) inResult;
        for (var i = 1; i < arr.Length; i += 2)
        {
            var dictionary = ToDictionary(arr[i + 1]);
            var vssRes = new VectorSimilarityResult
            {
                Key = (string) arr[i],
                Score = long.Parse(dictionary["__vector_score"]),
                Ticker = dictionary["ticker"]
            };
            result.Add(vssRes);
        }

        return result;
    } 
}