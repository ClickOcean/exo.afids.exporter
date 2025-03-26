using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Text.Json.Serialization;

[BsonIgnoreExtraElements]
public class AfidAttributes
{
    [JsonIgnore]
    public BsonObjectId Id { get; set; }

    [JsonPropertyName("afid")]
    [BsonElement("afid")]
    public long Afid { get; set; }

    [JsonPropertyName("afidType")]
    [BsonElement("afidType")]
    public string AfidType { get; set; }

    [JsonPropertyName("afidSubType")]
    [BsonElement("afidSubType")]
    public string StageName { get; set; }

    [JsonPropertyName("afidSubType")]
    [BsonElement("afidSubType")]
    public string Campaign { get; set; }

    [JsonPropertyName("afidSubType")]
    [BsonElement("afidSubType")]
    public string AdvGroup { get; set; }
}
