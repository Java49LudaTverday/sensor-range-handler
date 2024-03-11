package telran.aws.lambda;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


public class SensorRangeProvider implements RequestStreamHandler{
	String connectionString = "mongodb+srv://ludachka22:<password>@cluster0.97s5ttc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0";
	private String dbName = "sensors";
	private String collectionName = "sensor-ranges";

	@SuppressWarnings("unchecked")
	@Override
	public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
		LambdaLogger logger = context.getLogger();
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));		
		
		JSONParser parser = new JSONParser();
		String response = null;
		try {
			Map<String, Object> mapInput = (Map<String, Object>) parser.parse(reader);
			logger.log("trace: mapInput " + mapInput.toString());
			Map<String, Object> mapParameters = (Map<String, Object>) mapInput.get("pathParameters");
			if(mapParameters == null) {
				throw new IllegalArgumentException("request doesn`t contain parameters");
			}
			logger.log("debug: mapParameters " + mapParameters.toString());
			String sensorIdStr = (String) mapParameters.get("id");
			if(sensorIdStr == null) {
				throw new IllegalArgumentException("no id parameter");
			}
			long sensorId = Long.parseLong(sensorIdStr);
			logger.log("received sensorID " + sensorId);
			//********
			MongoClient mongoClient = MongoClients.create(connectionString);
			MongoDatabase mongoDB = mongoClient.getDatabase(dbName);
			MongoCollection<Document> mongoCollection = mongoDB.getCollection(collectionName);
			Document document = mongoCollection.find(new Document("_id", sensorId)).first();
			//*********
			response = createResponse(document.toJson(), 200);
			logger.log("debug: response is " + response);
		} catch (Exception e) {
			String body = e.toString();
			logger.log("error:" + body);
			response = createResponse(body, 400);
		}
		PrintStream printStream = new PrintStream(output);
		printStream.println(response);
		printStream.close();
		/*
		MongoClient mongoClient = MongoClients.create(URL);
		MongoDatabase mongoDB = mongoClient.getDatabase(dbName);
//		MongoCollection<SensorRangeDto> mongoCollection = mongoDB.getCollection(collectionName, SensorRangeDto.class );
//		SensorRangeDto document = mongoCollection.find(eq("_id", sensorId)).first();
		MongoCollection<Document> mongoCollection = mongoDB.getCollection(collectionName);
		Document document = mongoCollection.find(new Document("_id", sensorId)).first();
		*/
	}
	
	private String createResponse (String body, int statusCode) {
		HashMap<String, Object> map = new HashMap<>();
		map.put("status", statusCode);
		map.put("body", body);
		String jsonStr = JSONObject.toJSONString(map);
		return jsonStr;
	}

}
