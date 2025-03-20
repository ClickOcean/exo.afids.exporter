using MongoDB.Bson;
using MongoDB.Driver;

namespace ExportConsole.Services
{
    public class MongoDbService : IMongoDbService
    {
        public async Task<IMongoDatabase> ConnectToDatabase(string host, string port, string username, string password, string databaseName)
        {
            Console.WriteLine("Connecting to MongoDB...");
            var mongoClient = GetMongoClient(host, port, username, password);
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

        private MongoClient GetMongoClient(string host, string port, string username, string password)
        {
            if (string.IsNullOrEmpty(username) || string.IsNullOrEmpty(password))
            {
                return new MongoClient($"mongodb://{host}:{port}");
            }

            return new MongoClient($"mongodb://{username}:{password}@{host}:{port}");
        }
    }
}