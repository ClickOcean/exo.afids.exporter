using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Text.Json.Serialization;

[BsonIgnoreExtraElements]
public class AfidAttributes
{
    [JsonIgnore]
    [BsonId]
    public BsonObjectId _id { get; set; }

    [BsonElement("afid")]
    [JsonPropertyName("afid")]
    public long Afid { get; set; }

    [BsonElement("afidType")]
    [JsonPropertyName("afidType")]
    public string AfidType { get; set; }

    [BsonElement("stageName")]
    [JsonPropertyName("stageName")]
    public string StageName { get; set; }

    [BsonElement("campaign")]
    [JsonPropertyName("campaign")]
    public string Campaign { get; set; }

    [BsonElement("advGroup")]
    [JsonPropertyName("advGroup")]
    public string AdvGroup { get; set; }
}
