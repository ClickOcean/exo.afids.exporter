using MongoDB.Bson;
using MongoDB.Driver;

namespace ExportConsole.Services
{
    public interface IMongoDbService
    {
        Task<IMongoDatabase> ConnectToDatabase(string mongoUrl);
        Task<IAsyncCursor<BsonDocument>> GetDocumentCursorWithDateFilter(
            IMongoCollection<BsonDocument> collection,
            DateTime? lastRunDate,
            int batchSize = 100);
        IMongoCollection<BsonDocument> GetCollection(IMongoDatabase database, string collectionName);
    }
}