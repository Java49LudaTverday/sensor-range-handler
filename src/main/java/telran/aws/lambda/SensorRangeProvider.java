package telran.aws.lambda;

import java.util.Map;
import java.util.NoSuchElementException;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class SensorRangeProvider implements RequestHandler<Map<String, Object>, SensorRange> {
	private final MongoClient mongoClient;
	private String dbName = System.getenv("DB_NAME");
	private String collectionName = System.getenv("COLLECTION_NAME");

	public SensorRangeProvider() {
		mongoClient = MongoClients.create(System.getenv("MONGODB_URI"));
	}

	@SuppressWarnings("unchecked")
	@Override
	public SensorRange handleRequest(Map<String, Object> event, Context context) {
		LambdaLogger logger = context.getLogger();
		Map<String, Object> mapParameters = (Map<String, Object>) event.get("pathParameters");
		if (mapParameters == null) {
			logger.log("error: request doesn`t contain parameters");
			throw new IllegalArgumentException("request doesn`t contain parameters");
		}
		logger.log("debug: mapParameters " + mapParameters.toString());
		String sensorIdStr = (String) mapParameters.get("id");
		if (sensorIdStr == null) {
			logger.log("error: no id parameter");
			throw new IllegalArgumentException("no id parameter");
		}
		long sensorId = Long.parseLong(sensorIdStr);
		logger.log("received sensorID " + sensorId);
		Document document = getDocument(sensorId);
		if (document == null) {
			logger.log("error: " + String.format("sensor with id %d doesn`t exist", sensorId));
			throw new NoSuchElementException(String.format("sensor with id %d doesn`t exist", sensorId));
		}
		SensorRange result = new SensorRange(Float.parseFloat(document.get("minValue").toString()),
				Float.parseFloat(document.get("maxValue").toString()));
		logger.log("debug: result is " + result);
		return result;
	}

	private Document getDocument(long sensorId) {
		MongoDatabase mongoDB = mongoClient.getDatabase(dbName);
		MongoCollection<Document> mongoCollection = mongoDB.getCollection(collectionName);
		Document document = mongoCollection.find(new Document("_id", sensorId)).first();
		return document;
	}
}

record SensorRange(float minValue, float maxValue) {
}