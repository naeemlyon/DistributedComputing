package miscUtils;

import java.util.HashMap;
import java.util.Map;

public class MRDPUtils {

	public static final String[] REDIS_INSTANCES = { "p0", "p1", "p2", "p3", "p4", "p6" };

    //This helper function parses the stack overflow into a Map for us.
    public static Map<String, String> transformXmlToMap(String xml) {
    	
        Map<String, String> map = new HashMap<String, String>();
        
        try {
        	
        	String[] tokens = xml.trim().split(",");
        	String key = tokens[0].trim();
        	String val = tokens[1].trim();
        	for (int i = 2; i < tokens.length; i++) {
        		val += "," + tokens[i];        		
        	}
        	map.put("k", key);
        	map.put("v", val);            
        } 
        catch (StringIndexOutOfBoundsException e) {
            System.err.println(xml);
        }
        
        return map;
    }
}
