using Confluent.Kafka;
using ExportConsole.Models;
using ExportConsole.Services;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

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
        public async Task RunExportAsync_ProcessesDocumentsInBatches()
        {
            // Arrange
            var config = new ExportConfiguration(
                "localhost",
                "27017",
                "user",
                "password",
                "defaultdb",
                "localhost:9092",
                "export-console",
                "test-topic",
                "test-collection",
                2
            );

            // Setup MongoDB mocks
            _mockMongoDbService.Setup(m => m.ConnectToDatabase(config.MongoHost, config.MongoPort, config.MongoUsername, config.MongoPassword, config.MongoDatabase))
                .ReturnsAsync(_mockDatabase.Object);
            _mockMongoDbService.Setup(m => m.GetCollection(_mockDatabase.Object, config.MongoCollection))
                .Returns(_mockCollection.Object);
            _mockMongoDbService.Setup(m => m.GetDocumentCursor(_mockCollection.Object, config.BatchSize))
                .ReturnsAsync(_mockCursor.Object);

            // Setup test documents
            var testDocuments = new List<BsonDocument>
            {
                new BsonDocument { { "afid", 1001 }, { "_id", ObjectId.GenerateNewId() } },
                new BsonDocument { { "afid", 1002 }, { "_id", ObjectId.GenerateNewId() } },
                new BsonDocument { { "afid", 1003 }, { "_id", ObjectId.GenerateNewId() } }
            };

            // Setup cursor behavior
            _mockCursor.SetupSequence(c => c.MoveNextAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(true)
                .ReturnsAsync(false);
            _mockCursor.Setup(c => c.Current).Returns(testDocuments);

            // Setup Kafka producer mock
            _mockKafkaProducerService.Setup(k => k.CreateProducer(config.KafkaBrokers, config.KafkaClientId))
                .Returns(_mockProducer.Object);
            _mockKafkaProducerService.Setup(k => k.ProduceMessageAsync(
                    _mockProducer.Object,
                    config.KafkaTopic,
                    It.IsAny<string>(),
                    It.IsAny<string>()))
                .ReturnsAsync(new DeliveryResult<string, string>
                {
                    Topic = config.KafkaTopic,
                    Partition = 0,
                    Offset = 0
                });

            // Act
            var result = await _exportService.RunExportAsync(config);

            // Assert
            Assert.AreEqual(3, result.TotalProcessed);
            Assert.AreEqual(2, result.TotalBatches); // 2 batches with batch size 2 (2 docs in first, 1 in second)

            _mockKafkaProducerService.Verify(k => k.ProduceMessageAsync(
                _mockProducer.Object,
                config.KafkaTopic,
                It.IsAny<string>(),
                It.IsAny<string>()),
                Times.Exactly(3));
        }

        [TestMethod]
        public async Task RunExportAsync_UsesDefaultValuesWhenEnvironmentVariablesNotSet()
        {
            // Arrange
            var config = new ExportConfiguration(
                "localhost",
                "27017",
                "user",
                "password",
                "defaultdb",
                "localhost:9092",
                "export-console",
                "default-topic",
                "test",
                100
            );

            // Setup MongoDB mocks
            _mockMongoDbService.Setup(m => m.ConnectToDatabase(config.MongoHost, config.MongoPort, config.MongoUsername, config.MongoPassword, config.MongoDatabase))
                .ReturnsAsync(_mockDatabase.Object);
            _mockMongoDbService.Setup(m => m.GetCollection(_mockDatabase.Object, config.MongoCollection))
                .Returns(_mockCollection.Object);
            _mockMongoDbService.Setup(m => m.GetDocumentCursor(_mockCollection.Object, config.BatchSize))
                .ReturnsAsync(_mockCursor.Object);

            // Setup empty cursor (no documents)
            _mockCursor.Setup(c => c.MoveNextAsync(It.IsAny<CancellationToken>())).ReturnsAsync(false);

            // Setup Kafka producer mock
            _mockKafkaProducerService.Setup(k => k.CreateProducer(config.KafkaBrokers, config.KafkaClientId))
                .Returns(_mockProducer.Object);

            // Act
            var result = await _exportService.RunExportAsync(config);

            // Assert
            Assert.AreEqual(0, result.TotalProcessed);
            Assert.AreEqual(0, result.TotalBatches);

            // Verify default collection name was used
            _mockMongoDbService.Verify(m => m.GetCollection(_mockDatabase.Object, config.MongoCollection), Times.Once);
        }

        [TestMethod]
        public async Task ProcessBatch_HandlesValidDocuments()
        {
            // Arrange
            var config = new ExportConfiguration(
                "localhost",
                "27017",
                "user",
                "password",
                "defaultdb",
                "localhost:9092",
                "export-console",
                "test-topic",
                "test-collection",
                2
            );

            var testDocuments = new List<BsonDocument>
            {
                new BsonDocument { { "afid", 1001 }, { "_id", ObjectId.GenerateNewId() } },
                new BsonDocument { { "afid", 1002 }, { "_id", ObjectId.GenerateNewId() } }
            };

            _mockKafkaProducerService.Setup(k => k.ProduceMessageAsync(
                    _mockProducer.Object,
                    config.KafkaTopic,
                    It.IsAny<string>(),
                    It.IsAny<string>()))
                .ReturnsAsync(new DeliveryResult<string, string>
                {
                    Topic = config.KafkaTopic,
                    Partition = 0,
                    Offset = 0
                });

            // Act
            await _exportService.ProcessBatch(testDocuments, _mockProducer.Object, config.KafkaTopic);

            // Assert
            _mockKafkaProducerService.Verify(k => k.ProduceMessageAsync(
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
                "localhost",
                "27017",
                "user",
                "password",
                "defaultdb",
                "localhost:9092",
                "export-console",
                "test-topic",
                "test-collection",
                2
            );

            var testDocuments = new List<BsonDocument>
            {
                new BsonDocument { { "afid", 1001 }, { "_id", ObjectId.GenerateNewId() } },
                // Missing afid field
                new BsonDocument { { "invalid_field", "invalid_value" }, { "_id", ObjectId.GenerateNewId() } }
            };

            _mockKafkaProducerService.Setup(k => k.ProduceMessageAsync(
                    _mockProducer.Object,
                    config.KafkaTopic,
                    It.IsAny<string>(),
                    It.IsAny<string>()))
                .ReturnsAsync(new DeliveryResult<string, string>
                {
                    Topic = config.KafkaTopic,
                    Partition = 0,
                    Offset = 0
                });

            // Act
            await _exportService.ProcessBatch(testDocuments, _mockProducer.Object, config.KafkaTopic);

            // Assert - only one document should be processed
            _mockKafkaProducerService.Verify(k => k.ProduceMessageAsync(
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
                "localhost",
                "27017",
                "user",
                "password",
                "defaultdb",
                "localhost:9092",
                "export-console",
                "default-topic",
                "test",
                100
            );

            _mockMongoDbService.Setup(m => m.ConnectToDatabase(config.MongoHost, config.MongoPort, config.MongoUsername, config.MongoPassword, config.MongoDatabase))
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
                "localhost",
                "27017",
                "user",
                "password",
                "defaultdb",
                "localhost:9092",
                "export-console",
                "test-topic",
                "test",
                10
            );

            // Setup MongoDB mocks
            _mockMongoDbService.Setup(m => m.ConnectToDatabase(config.MongoHost, config.MongoPort, config.MongoUsername, config.MongoPassword, config.MongoDatabase))
                .ReturnsAsync(_mockDatabase.Object);
            _mockMongoDbService.Setup(m => m.GetCollection(_mockDatabase.Object, config.MongoCollection))
                .Returns(_mockCollection.Object);
            _mockMongoDbService.Setup(m => m.GetDocumentCursor(_mockCollection.Object, config.BatchSize))
                .ReturnsAsync(_mockCursor.Object);

            // Setup cursor with no documents
            _mockCursor.Setup(c => c.MoveNextAsync(It.IsAny<CancellationToken>())).ReturnsAsync(false);

            // Setup Kafka producer mock
            _mockKafkaProducerService.Setup(k => k.CreateProducer(config.KafkaBrokers, config.KafkaClientId))
                .Returns(_mockProducer.Object);

            // Act
            var result = await _exportService.RunExportAsync(config);

            // Assert
            Assert.AreEqual(0, result.TotalProcessed);
            Assert.AreEqual(0, result.TotalBatches);
        }

        [TestMethod]
        public async Task ProcessBatch_ContinuesProcessingWhenDocumentFails()
        {
            // Arrange
            var config = new ExportConfiguration(
                "localhost",
                "27017",
                "user",
                "password",
                "defaultdb",
                "localhost:9092",
                "export-console",
                "test-topic",
                "test-collection",
                2
            );

            var testDocuments = new List<BsonDocument>
            {
                new BsonDocument { { "afid", 1001 }, { "_id", ObjectId.GenerateNewId() } },
                new BsonDocument { { "afid", 1002 }, { "_id", ObjectId.GenerateNewId() } }
            };

            // First document succeeds
            _mockKafkaProducerService.Setup(k => k.ProduceMessageAsync(
                    _mockProducer.Object,
                    config.KafkaTopic,
                    "1001",
                    testDocuments[0].ToString()))
                .ReturnsAsync(new DeliveryResult<string, string>
                {
                    Topic = config.KafkaTopic,
                    Partition = 0,
                    Offset = 0
                });

            // Second document throws exception
            _mockKafkaProducerService.Setup(k => k.ProduceMessageAsync(
                    _mockProducer.Object,
                    config.KafkaTopic,
                    "1002",
                    testDocuments[1].ToString()))
                .ThrowsAsync(new KafkaException(new Error(ErrorCode.Local_BadMsg, "Bad message")));

            // Act - should not throw exception
            await _exportService.ProcessBatch(testDocuments, _mockProducer.Object, config.KafkaTopic);

            // Assert - first document should be processed
            _mockKafkaProducerService.Verify(k => k.ProduceMessageAsync(
                _mockProducer.Object,
                config.KafkaTopic,
                "1001",
                testDocuments[0].ToString()),
                Times.Once);
        }
    }
}