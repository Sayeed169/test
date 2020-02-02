package ast.hylite.ictbus;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONException;
import org.json.JSONObject;

public class JSONSerDes implements Serializer<JSONObject>, Deserializer<JSONObject>{

	@Override
	public byte[] serialize(String topic, JSONObject data) {

		try {
			return data.toString().getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			return null;
		}

	}

	@Override
	public JSONObject deserialize(String topic, byte[] data) {

		try {
			return new JSONObject(new String(data, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			return null;
		} catch (JSONException e) {
			return null;
		}
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub

	}







}
