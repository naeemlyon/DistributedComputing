package miscUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CommonUtil {
		
	////////////////////////////////////////////////////////////////////////////////
	/*
	 * Input: String // delimited text
	 * Input: String // delimiter
	 * Output: Map
	 * converts delimited text into HashMap 
	 */
	public static Map<Integer, Integer> PopulateMap(String input, String DLMT) {
		Map<Integer,Integer> map = new HashMap<Integer,Integer>();
		
		if (input.contains("null"))
			return map;
		
		String[] Arr = input.trim().split(DLMT);
		int num = 0;
        for (int i=0; i<Arr.length; i++) {
        	num = Integer.parseInt(Arr[i]); 
        	map.put(num, num);
        }
		return map;
	}
	
	///////////////////////////////////////////////////////////////////////////////
    // Separate common ids to be used in mapper.
    public static Map<String, String> SeparateCommonIDs(String input, String K, String V, String DLMT) {    	
    	
   // 	System.out.println(input + " : " + K + " : " + V);
    	Map<String, String> map = new HashMap<String, String>();
    	String key = "", val = "", tmp = "";
    	int i=0, num=0;     
    
    	String[] arrK = K.split(DLMT);
    	String[] arrV = V.split(DLMT);
    	String[] tokens = input.trim().split(DLMT);
    	
    	try {
    	
	    	if (K.equals("null") == false) {
	    		
		    	for (i=0; i<arrK.length; i++) {
		    		num = Integer.parseInt(arrK[i]);
		    		tmp = tokens[num].trim();  
		    		key += DLMT +  tmp;
		    	}
		    	// remove first DLMT
		    	if (key.length() > 1) {
		    		key = key.substring(1);
		    	}
	    	}	    	
	    	
	    	if (V.equals("null") == false) {
	        	
	    		for (i=0; i<arrV.length; i++) {
	        		num = Integer.parseInt(arrV[i]);
	        		tmp = tokens[num].trim();  
	        		val += DLMT +  tmp;
	        	}
	        	
	        	// remove first DLMT
	    		if (val.length() > 1) {
	    			  val = val.substring(1);
	        	}    		
	    	}	

    	} catch (IndexOutOfBoundsException e) {
    		System.out.println(input + " : " + K + " : " + V);
    		System.out.println("IndexOutOfBoundsException: " + e.getMessage());
    		// System.err.println("IndexOutOfBoundsException: " + e.getMessage());
    	}
    	
    	map.put("k", key);
    	map.put("v", val);
        return map;
    }
	
	
    /**
   	 * Delete a folder on the HDFS. This is an example of how to interact
   	 * with the HDFS using the Java API. You can also interact with it
   	 * on the command line, using: hdfs dfs -rm -r /path/to/delete
   	 * 
   	 * @param conf a Hadoop Configuration object
   	 * @param folderPath folder to delete
   	 * @throws IOException
   	 */
	////////////////////////////////////////////////////////////////////////////////
	public void deleteFolder(Configuration conf, Path path) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		//Path path = new Path(folderPath); // if folderPath is string argument 
		if(fs.exists(path)) {
			fs.delete(path,true);
		}
	}
	
	
	////////////////////////////////////////////////////////////////////////////////
	/**
	 * Split string and extract the numeric value 
	 * Populate the Hashset 
	 * @param String varString 
	 * @return Hashset	 
	 */
		
	public HashSet<Integer> SplitNumericVariables(String varString, String delimiter){
		HashSet<Integer> hs = new HashSet<Integer>();
	    StringTokenizer tok = new StringTokenizer(varString,",");
	    while(tok.hasMoreElements())
	    	hs.add(Integer.parseInt(tok.nextToken()));
		return hs;
	}
	/////////////////////////////////////////////////////////////////////////////
	
	public HashSet<String> SplitStringVariables(String varString, String delimiter){
		HashSet<String> hs = new HashSet<String>();
	    StringTokenizer tok = new StringTokenizer(varString,",");
	    while(tok.hasMoreElements())
	    	hs.add(tok.nextToken());
		return hs;
	}
	
   //////////////////////////////////////////////////////////////////////////////
   public static void WriteFiles(String Data, String pth) {	
		try {
			File file = new File(pth);
			FileWriter fileWriter = new FileWriter(file);
			fileWriter.write(Data);
			fileWriter.flush();
			fileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

    /////////////////////////////////////////////////////////////////////////
    public static Map <String, String> toMap(String input, boolean ReserverPosition) {
    	Map <String, String> map = new HashMap <String, String>();
    	String[] arr = input.split(SC.DLMT); 
    	int i =0;
    	if (ReserverPosition) {
    		for (i=0; i<arr.length; i++) {
        		map.put(arr[i].trim(), i + "" ); // useful for writing bash script
        	}
    		
    	} else {
    		for (i=0; i<arr.length; i++) {
        		map.put(arr[i].trim(), arr[i].trim()); // useful for developing BigMap
        	}    		
    	}
    //	System.out.println("\n" + map + NL);    	
    	return map;
    }
    
	////////////////////////////////////////////////////////////////////////////
	// solve the first DLMT in situations when there is no UnCommon features
	public static String CleanFeatureIndicesString(String input) {
		String ret = input;
		while ( (ret.startsWith(SC.DLMT) == true)){
			ret = ret.substring(1);
		}
		return ret;
	}
	
	////////////////////////////////////////////////////////////////////////////
	public static void DisplayMap(Map<String, String> inp ) {
		String out = "";		
		Iterator<Map.Entry<String,String>> it = inp.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, String> pair = it.next();
			String[] arr = pair.getKey().split(SC.COLON);
			out = arr.length + " :: ";
			//out += pair.getValue().split(SC.COLON).length;			 
			System.out.println(out + "...." + pair.getKey() + SC.NL + pair.getValue() + "\n");
		}
	}
   /////////////////////////////////////////////////////////////////////////////
	// Serialize the object in hard disk
	// to speed up the processing on next call of the program
	public static int Serialize(Map<String, String> map, String fn) {
		int ret = 1;		
		try
        {
           FileOutputStream fos =
              new FileOutputStream(fn);
           ObjectOutputStream oos = new ObjectOutputStream(fos);
           oos.writeObject(map);
           oos.close();
           fos.close();
           System.out.printf("Serialized HashMap is saved at " + fn);
        }catch(IOException ioe)
        {
           ioe.printStackTrace();
           ret = -1;
        }		
		return ret;
	}
	
	// pull out the serialized object 
	// optimized way of running program
	@SuppressWarnings("unchecked")
	public static HashMap<String, String> Deserialize(String fn) {
		
		HashMap<String, String> tmpMap = new HashMap<String, String>();
		try
	      {
	         FileInputStream fis = new FileInputStream(fn);
	         ObjectInputStream ois = new ObjectInputStream(fis);
	         tmpMap = (HashMap<String, String>) ois.readObject();
	         ois.close();
	         fis.close();
	      }catch(IOException ioe)
	      {
	         ioe.printStackTrace();
	         System.out.println("Kindly develop the map (one time only), then you can use this option on every start up of this program");
	         return null;
	      }catch(ClassNotFoundException c)
	      {	         
	         c.printStackTrace();
	         return null;
	      }
	      //System.out.println("Deserialized HashMap..");
		return tmpMap;	
	}
	

	////////////////////////////////////////////////////////////////////////////////////////

	public static Boolean isNullOrBlank(String s) {
	   return (s==null || s.trim().equals(""));
	}

	
	////////////////////////////////////////////////////////////////////////////////////////
	/*
	 * input of the command line
	 * input: // hadoop jar nb.jar reducejoin.JoinDriver test train out inner -D KeysA="7" -d ValA="0,1,2,3,4,5,6" -D DLMT "," -D KeysB="6" -D ValB="0,1,2,3,4,5,7"
	 * No worries about double.quotes, linux eliminated them automatically before they reach at main()
	 * output is to detect the value of toFind (KyesA) --> 7,6,3,6
	 * 
	 */
	////////////////////////////////////////////////////////////////////////////////////////
	public String PrepareConfigurationData(String[] inp, String toFind) {		
		String ret = "";
		String input = "";
		int i=0;
		for (i=0; i<inp.length; i++) {
			input += inp[i] + " ";
		}			
		
		// enable the terminal command -D case-in-sensitive
		input = input.replace(" -d ", " -D ");		
		// System.out.println("\n\n" + input.length() + "\n\n");
		
		String[] arr = input.split("-D");
		input = ""; // bydefault toFind is not present
		for (i=0; i<arr.length; i++) {
			if (arr[i].contains(toFind) == true) {
				input = arr[i].replace(toFind, "");
				break;
			}
		}		
		
		if (input.length() == 0) {
			return  "'" + toFind + "' is missing in the parameters at command line";
		}
				
		ret = input.replace("=", "");
		// System.out.println("\n\n" + input + "\n\n");
		return ret.trim();
	}
		
	////////////////////////////////////////////////////////////////////////////////////////
		
	
	
}
