using MongoDB.Bson;
using MongoDB.Driver;

namespace ExportConsole.Services
{
    public interface IMongoDbService
    {
        Task<IMongoDatabase> ConnectToDatabase(string mongoUrl);
        Task<IAsyncCursor<BsonDocument>> GetDocumentCursor(IMongoCollection<BsonDocument> collection, int batchSize = 100);
        IMongoCollection<BsonDocument> GetCollection(IMongoDatabase database, string collectionName);
    }
}