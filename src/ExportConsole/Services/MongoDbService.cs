using MongoDB.Bson;
using MongoDB.Driver;

namespace ExportConsole.Services
{
    public class MongoDbService : IMongoDbService
    {
        public async Task<IMongoDatabase> ConnectToDatabase(string mongoUrl)
        {
            Console.WriteLine("Connecting to MongoDB...");
            var mongoClient = new MongoClient(mongoUrl);

            // Extract the database name from the URL
            var mongoUrlBuilder = new MongoUrlBuilder(mongoUrl);
            var databaseName = mongoUrlBuilder.DatabaseName;

            if (string.IsNullOrEmpty(databaseName))
            {
                throw new ArgumentException("Database name must be included in the connection URL.");
            }

            var database = mongoClient.GetDatabase(databaseName);

            // Ping the server to verify connection
            await database.RunCommandAsync((Command<BsonDocument>)"{ping:1}");
            Console.WriteLine("Successfully connected to MongoDB");

            return database;
        }

        public IMongoCollection<BsonDocument> GetCollection(IMongoDatabase database, string collectionName)
        {
            return database.GetCollection<BsonDocument>(collectionName);
        }

        public Task<IAsyncCursor<BsonDocument>> GetDocumentCursor(IMongoCollection<BsonDocument> collection, int batchSize = 100)
        {
            return collection.Find(new BsonDocument(), new() { BatchSize = batchSize }).ToCursorAsync();
        }
    }
}