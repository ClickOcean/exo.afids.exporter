using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization.Attributes;

[BsonIgnoreExtraElements]
public class AfidAttributes
{
    [BsonElement("afid")]
    public long Afid { get; set; }

    public BsonObjectId Id { get; set; }
}
