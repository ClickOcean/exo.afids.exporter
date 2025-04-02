using Confluent.Kafka;
using ExportConsole.Services;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;

namespace ExportConsole.Tests.Services
{
    [TestClass]
    public class ExportServiceTests
    {
        private Mock<IMongoDbService> _mockMongoDbService;
        private Mock<IKafkaProducerService> _mockKafkaProducerService;
        private ExportService _exportService;
        private Mock<IProducer<string, string>> _mockProducer;
        private Mock<IMongoDatabase> _mockDatabase;
        private Mock<IMongoCollection<BsonDocument>> _mockCollection;
        private Mock<IAsyncCursor<BsonDocument>> _mockCursor;

        [TestInitialize]
        public void Setup()
        {
            _mockMongoDbService = new Mock<IMongoDbService>();
            _mockKafkaProducerService = new Mock<IKafkaProducerService>();
            _mockProducer = new Mock<IProducer<string, string>>();
            _mockDatabase = new Mock<IMongoDatabase>();
            _mockCollection = new Mock<IMongoCollection<BsonDocument>>();
            _mockCursor = new Mock<IAsyncCursor<BsonDocument>>();

            _exportService = new ExportService(_mockMongoDbService.Object, _mockKafkaProducerService.Object);
        }

        [TestMethod]
        public async Task RunExportAsync_UsesDefaultValuesWhenEnvironmentVariablesNotSet()
        {
            // Arrange
            var config = new ExportConfiguration(
                "mongodb://localhost:27017",
                "testCollection",
                "localhost:9092",
                "testClientId",
                "testTopic",
                100
            )
            {
                IsInitialRun = true
            };

            // Setup MongoDB mocks
            _mockMongoDbService.Setup(m => m.ConnectToDatabase(config.MongoUrl))
                .ReturnsAsync(_mockDatabase.Object);
            _mockMongoDbService.Setup(m => m.GetCollection(_mockDatabase.Object, config.MongoCollection))
                .Returns(_mockCollection.Object);
            _mockMongoDbService.Setup(m => m.GetDocumentCursorWithDateFilter(_mockCollection.Object, It.IsAny<DateTime?>(), config.BatchSize))
                .ReturnsAsync(_mockCursor.Object);

            // Setup empty cursor (no documents)
            _mockCursor.Setup(c => c.MoveNextAsync(It.IsAny<CancellationToken>())).ReturnsAsync(false);

            // Setup Kafka producer mock
            _mockKafkaProducerService.Setup(k => k.CreateProducer(config.KafkaBrokers, config.KafkaClientId, config.BatchSize, It.IsAny<string?>(), It.IsAny<string?>(), It.IsAny<string?>()))
                .Returns(_mockProducer.Object);

            // Act
            var result = await _exportService.RunExportAsync(config);

            // Assert
            Assert.AreEqual(0, result.TotalProcessed);

            // Verify default collection name was used
            _mockMongoDbService.Verify(m => m.GetCollection(_mockDatabase.Object, config.MongoCollection), Times.Once);
        }

        [TestMethod]
        public async Task ProcessBatch_HandlesValidDocuments()
        {
            // Arrange
            var config = new ExportConfiguration(
                "mongodb://localhost:27017",
                "testCollection",
                "localhost:9092",
                "testClientId",
                "testTopic",
                2
            )
            {
                IsInitialRun = true
            };

            var testDocuments = new List<BsonDocument>
            {
                new() { { "afid", 1001 }, { "_id", ObjectId.GenerateNewId() } },
                new() { { "afid", 1002 }, { "_id", ObjectId.GenerateNewId() } }
            };

            // Act
            _exportService.ProcessDocument(testDocuments[0], _mockProducer.Object, config.KafkaTopic);
            _exportService.ProcessDocument(testDocuments[1], _mockProducer.Object, config.KafkaTopic);

            // Assert
            _mockKafkaProducerService.Verify(k => k.ProduceMessage(
                _mockProducer.Object,
                config.KafkaTopic,
                It.IsAny<string>(),
                It.IsAny<string>()),
                Times.Exactly(2));
        }

        [TestMethod]
        public async Task ProcessBatch_SkipsInvalidDocuments()
        {
            // Arrange
            var config = new ExportConfiguration(
                "mongodb://localhost:27017",
                "testCollection",
                "localhost:9092",
                "testClientId",
                "testTopic",
                2
            )
            {
                IsInitialRun = true
            };

            var testDocuments = new List<BsonDocument>
            {
                new() { { "afid", 1001 }, { "_id", ObjectId.GenerateNewId() } },
                // Missing afid field
                new() { { "invalid_field", "invalid_value" }, { "_id", ObjectId.GenerateNewId() } }
            };

            // Act
            _exportService.ProcessDocument(testDocuments[0], _mockProducer.Object, config.KafkaTopic);
            _exportService.ProcessDocument(testDocuments[1], _mockProducer.Object, config.KafkaTopic);

            // Assert - only one document should be processed
            _mockKafkaProducerService.Verify(k => k.ProduceMessage(
                _mockProducer.Object,
                config.KafkaTopic,
                It.IsAny<string>(),
                It.IsAny<string>()),
                Times.Once);
        }

        [TestMethod]
        [ExpectedException(typeof(Exception))]
        public async Task RunExportAsync_PropagatesExceptionsFromMongoDb()
        {
            // Arrange
            var config = new ExportConfiguration(
                "mongodb://localhost:27017",
                "testCollection",
                "localhost:9092",
                "testClientId",
                "testTopic",
                100
            )
            {
                IsInitialRun = true
            };

            _mockMongoDbService.Setup(m => m.ConnectToDatabase(config.MongoUrl))
                .ThrowsAsync(new Exception("MongoDB connection failed"));

            // Act
            await _exportService.RunExportAsync(config);

            // Assert - method should throw exception
        }

        [TestMethod]
        public async Task RunExportAsync_HandlesEmptyBatch()
        {
            // Arrange
            var config = new ExportConfiguration(
                "mongodb://localhost:27017",
                "testCollection",
                "localhost:9092",
                "testClientId",
                "testTopic",
                10
            )
            {
                IsInitialRun = true
            };

            // Setup MongoDB mocks
            _mockMongoDbService.Setup(m => m.ConnectToDatabase(config.MongoUrl))
                .ReturnsAsync(_mockDatabase.Object);
            _mockMongoDbService.Setup(m => m.GetCollection(_mockDatabase.Object, config.MongoCollection))
                .Returns(_mockCollection.Object);
            _mockMongoDbService.Setup(m => m.GetDocumentCursorWithDateFilter(_mockCollection.Object, It.IsAny<DateTime?>(), config.BatchSize))
                .ReturnsAsync(_mockCursor.Object);

            // Setup empty cursor (no documents)
            _mockCursor.Setup(c => c.MoveNextAsync(It.IsAny<CancellationToken>())).ReturnsAsync(false);

            // Setup Kafka producer mock
            _mockKafkaProducerService.Setup(k => k.CreateProducer(config.KafkaBrokers, config.KafkaClientId, config.BatchSize, It.IsAny<string?>(), It.IsAny<string?>(), It.IsAny<string?>()))
                .Returns(_mockProducer.Object);

            // Act
            var result = await _exportService.RunExportAsync(config);

            // Assert
            Assert.AreEqual(0, result.TotalProcessed);
        }

        [TestMethod]
        public async Task ProcessBatch_ContinuesProcessingWhenDocumentFails()
        {
            // Arrange
            var config = new ExportConfiguration(
                "mongodb://localhost:27017",
                "testCollection",
                "localhost:9092",
                "testClientId",
                "testTopic",
                2
            )
            {
                IsInitialRun = true
            };

            var testDocuments = new List<BsonDocument>
            {
                new() { { "afid", 1001 }, { "_id", ObjectId.GenerateNewId() } },
                new() { { "afid", 1002 }, { "_id", ObjectId.GenerateNewId() } }
            };

            // Second document throws exception
            _mockKafkaProducerService.Setup(k => k.ProduceMessage(
                    _mockProducer.Object,
                    config.KafkaTopic,
                    "1002",
                    It.IsAny<string>()))
                .Throws(new KafkaException(new Error(ErrorCode.Local_BadMsg, "Bad message")));

            // Act - should not throw exception
            _exportService.ProcessDocument(testDocuments[0], _mockProducer.Object, config.KafkaTopic);
            _exportService.ProcessDocument(testDocuments[1], _mockProducer.Object, config.KafkaTopic);

            // Assert - first document should be processed
            _mockKafkaProducerService.Verify(k => k.ProduceMessage(
                _mockProducer.Object,
                config.KafkaTopic,
                "1001",
                It.IsAny<string>()),
                Times.Once);
        }
    }
}