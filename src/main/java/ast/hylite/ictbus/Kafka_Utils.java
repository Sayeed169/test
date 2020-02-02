package ast.hylite.ictbus;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.json.JSONException;
import org.json.JSONObject;

public class Kafka_Utils {
	
	/**
	 * 
	 * Convert JSON key into double value
	 * 
	 * @param json - message as JSON object
	 * @param key - JSON key to extract value
	 * @return int value
	 * @throws JSONException
	 */
	public Integer key2int(JSONObject json, String key) throws JSONException {
		
		// Set default value
		Integer value = (int) Double.NaN;
		
		// Extract key information
		if (json.isNull(key)) {
		} else {
			value = json.getInt(key);
		}
		
		return value;
		
	}
	
	/**
	 * 
	 * Convert JSON key into double value
	 * 
	 * @param json - message as JSON object
	 * @param key - JSON key to extract value
	 * @return double value
	 * @throws JSONException
	 */
	public double key2double(JSONObject json, String key) throws JSONException {
		
		// Set default value
		Double value = Double.NaN;
		
		// Extract key information
		if (json.isNull(key)) {
		} else {
			value = json.getDouble(key);
		}
		
		return value;
		
	}
	
	/**
	 * 
	 * Convert JSON key into string
	 * 
	 * @param json - message as JSON object
	 * @param key - JSON key to extract value
	 * @return String value
	 * @throws JSONException
	 */
	public String key2string(JSONObject json, String key) throws JSONException {
		
		// Set default value
		String value = "";
		
		// Extract key information
		if (json.isNull(key)) {
		} else {
			value = json.getString(key);
		}
		
		return value;
		
	}
	
	/**
	 * 
	 * Convert JSON key into timestamp from string timestamp with format "YYYY-MM-dd HH:mm:ss.SSSSSS"
	 * 
	 * @param json - message as JSON object
	 * @param key - JSON key to extract time stamp (ts)
	 * @return Date ts
	 * @throws JSONException
	 * @throws ParseException
	 */
	public Date key2datefromstr(JSONObject json, String key) throws JSONException, ParseException {
		
		// Set default value
		String ts_str = "";
		Date ts = Calendar.getInstance().getTime();
		
		// Set formatter
		DateFormat formatter = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss.SSSSSS");
		
		// Extract key information
		if (json.isNull(key)) {
		} else {
			ts_str = json.getString(key);
			ts = formatter.parse(ts_str);
		}
		
		return ts;
		
	}
	
	/**
	 * 
	 * Convert JSON key into Java date from unix timestamp as long
	 * 
	 * @param json - message as JSON object
	 * @param key - JSON key to extract time stamp (ts)
	 * @return Date
	 * @throws JSONException
	 */
	public Date key2datefromlong(JSONObject json, String key) throws JSONException {
		
		return new Date(json.getLong(key));
		
	}
	
	/**
	 * 
	 * Check if JSON key is null and return JSON null
	 * 
	 * @param key - JSON key to extract
	 * @return
	 */
	public Object key2null(Object key) {
		
		if (key == null) {
			return JSONObject.NULL;
		} else {
			return key;
		}
	
	}

	/**
	 * 
	 * Convert Java date into unix timestamp as long
	 * 
	 * @param timestamp
	 * @return
	 */
	public Long date2long(Date timestamp) {
				
		return timestamp.getTime();
		
	}
	
	/**
	 * 
	 * Rename key of JSON object
	 * 
	 * @param json - JSON object
	 * @param oldkey - key to be renamed / removed
	 * @param newkey - new key
	 * @throws JSONException
	 */
	public void renamekey(JSONObject json, String oldkey, String newkey) throws JSONException {
		
		json.put(newkey, json.get(oldkey));
		json.remove(oldkey);
		
	}

}
