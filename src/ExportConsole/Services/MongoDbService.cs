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

        public Task<IAsyncCursor<BsonDocument>> GetDocumentCursorWithDateFilter(
            IMongoCollection<BsonDocument> collection, 
            DateTime? lastRunDate,
            int batchSize = 100)
        {
            FilterDefinition<BsonDocument> filter;

            if (lastRunDate.HasValue)
            {
                var lastRunDateBson = new BsonDateTime(lastRunDate.Value);
                filter = Builders<BsonDocument>.Filter.Gt("updated", lastRunDateBson);
                
                Console.WriteLine($"Filtering documents created or updated after: {lastRunDate.Value}");
            }
            else
            {
                // If no last run date is available, get all documents
                filter = new BsonDocument();
                Console.WriteLine("No date filter applied - retrieving all documents");
            }

            return collection.Find(filter, new() { BatchSize = batchSize }).ToCursorAsync();
        }
    }
}