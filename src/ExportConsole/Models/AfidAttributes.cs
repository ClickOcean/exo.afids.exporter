using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization.Attributes;

[BsonIgnoreExtraElements]
public class AfidAttributes
{
    [BsonElement("afid")]
    public long Afid { get; set; }

    public BsonObjectId Id { get; set; }

    //[BsonElement("afidType")]
    //public string AfidType { get; set; }

    //[BsonElement("stageName")]
    //public string StageName { get; set; }

    //[BsonElement("campaign")]
    //public string Campaign { get; set; }

    //[BsonElement("advGroup")]
    //public string AdvGroup { get; set; }
}
