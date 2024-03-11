package telran.aws.lambda;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.json.simple.JSONObject;


public class SensorRangeProvider implements RequestHandler<Map<String, Object>, String> {
	private final MongoClient mongoClient;
	private String dbName = "sensors";
	private String collectionName = "sensor-ranges";

	public SensorRangeProvider() {
		mongoClient = MongoClients.create(System.getenv("MONGODB_URI"));
	}

	@SuppressWarnings("unchecked")
	@Override
	public String handleRequest(Map<String, Object> event, Context context) {
		LambdaLogger logger = context.getLogger();
		String response = null;
		try {
			Map<String, Object> mapParameters = (Map<String, Object>) event.get("pathParameters");
			if (mapParameters == null) {
				throw new IllegalArgumentException("request doesn`t contain parameters");
			}
			logger.log("debug: mapParameters " + mapParameters.toString());
			String sensorIdStr = (String) mapParameters.get("id");
			if (sensorIdStr == null) {
				throw new IllegalArgumentException("no id parameter");
			}
			long sensorId = Long.parseLong(sensorIdStr);
			logger.log("received sensorID " + sensorId);
			//**********
			MongoDatabase mongoDB = mongoClient.getDatabase(dbName);
			MongoCollection<Document> mongoCollection = mongoDB.getCollection(collectionName);
			Document document = mongoCollection.find(new Document("_id", sensorId)).first();
			// *********
			response = createResponse(document.toJson(), 200);
			logger.log("debug: response is " + response);
		} catch (Exception e) {
			String body = e.toString();
			logger.log("error:" + body);
			response = createResponse(body, 400);
		}
		return response;
	}

	private String createResponse(String body, int statusCode) {
		HashMap<String, Object> map = new HashMap<>();
		map.put("status", statusCode);
		map.put("body", body);
		String jsonStr = JSONObject.toJSONString(map);
		return jsonStr;
	}

}
