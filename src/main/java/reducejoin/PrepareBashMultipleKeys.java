package reducejoin;

import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.fs.*;

import fileIO.*;
import miscUtils.*;

/*
 * SQL terminology, the default value of all = FALSE -> inner join. 
 * all.x = TRUE -> a left (outer) join // we need this case
 * all.y = TRUE -> right (outer) join
 * (all = TRUE -> (full) outer join. *  
 */

public class PrepareBashMultipleKeys {	
	// local folders	
	static Map <String,String> SchemaMap = new HashMap <String, String>();
	static Map <String,String> SM = new HashMap <String, String>();
	static Map <String,String> SchemaFinal = new HashMap <String, String>();
	// see FileListing.java for detail of the folder input / output
	
	///////////////////////////////////////////////////////////////////////
	
	public static void main(String[] args) {
		boolean firstTime = false;
		BuildAllBash(firstTime);
		WriteBashMultipleKeys.Run(SchemaFinal, SM);   
	}       
	
	/////////////////////////////////////////////////////////////////////////////
	@SuppressWarnings("unused")
	private static String BuildAllBash(boolean firstTime) {		
		String ret = "";    	
    	// only one time and then copy output folder data2 to hdfs
		if (firstTime ) {
			ret = FileListing.ProcessFiles();	
			// System.out.println(ret);
		}
    	   	    	
    	FileStatus[] FileList = FileListing.ListAllFiles("data2");
    	SchemaMap = FileListing.PopulateSchemaMap();
    	/*
    	 * before writing SchemaMap will be exhausted
    	 * SM is a duplicate to be used for writing the bash file
    	 */
    	SM.putAll(SchemaMap);
    	
    	ret = BuildAllBash();   	   	
    	Map.Entry<String,String> pair = null;
    	//System.out.println(NL + pair.getKey() + NL + NL + pair.getValue() + NL);
    	
    	while (SchemaMap.size() > 0) {
    		pair = DevelopdataForNextRound (ret);
    		ret = BuildAllBash(pair);
    		System.out.println(SchemaMap.size() +  " -:- " + SchemaFinal.size());
    	}
    	
    	ret = "SchemaFinal created...";
    	System.out.println(ret);		
    	//CommonUtil.DisplayMap (SchemaFinal);
		return ret;
	}
		
	/////////////////////////////////////////////////////////////////////////////
	private static String BuildAllBash(Entry<String, String> p) {
		String ret = "";
		Iterator <Map.Entry<String,String>> t = SchemaMap.entrySet().iterator();
		while (t.hasNext()) {
			Entry<String, String> pair = t.next();
			 int found = IntersectionHeader (pair, p, true);
		    	 if (found == 1) {   		    		 
		    		 return pair.getKey() + SC.COLON + p.getKey();
		    	 }		    
		}		
		return ret;
	}	

	/////////////////////////////////////////////////////////////////////////////
	private static Map.Entry<String,String> DevelopdataForNextRound (String input) {
		Map.Entry<String,String> pair = null;
		String[] arr = input.split(SC.COLON);
		int i=0;
		for (i=0; i<arr.length; i++) {
			if (SchemaMap.containsKey(arr[i]) == true) {
				SchemaMap.remove(arr[i].trim());	
			}
		}
		
		Iterator<Map.Entry<String, String>> t = SchemaFinal.entrySet().iterator();
		while (t.hasNext()) {
			pair = t.next();
			if (pair.getKey().equals(input) == true) {
				return pair;
			}
		}		
		return pair;
	}
	
	
	////////////////////////////////////////////////////////////////////////////
	private static String BuildAllBash() {
		String ret = "";
    	
    	Map <String,String> tmpMap = SchemaMap;
    	Iterator <Map.Entry<String, String>> t1 = tmpMap.entrySet().iterator();    	
    	Iterator <Map.Entry<String, String>> t2 = SchemaMap.entrySet().iterator();
    	
    	while (t1.hasNext()) {
    		Entry<String, String> pair1 = t1.next();
  		    String ky1 = pair1.getKey().toString();  			
    		while (t2.hasNext()) {
   		     Entry<String, String> pair2 = t2.next();
   		     String ky2 = pair2.getKey().toString();
   		     //System.out.println(pair1.getValue() + "......." + pair2.getValue());
   		     
   		     if (!ky1.equals(ky2)) {
   		    	 int found = IntersectionHeader (pair1, pair2, false);
   		    	 if (found == 1) {   		    		 
   		    		 return pair1.getKey() + SC.COLON + pair2.getKey();
   		    	 }
   		     }
    		}    		
    	}
    	 				
		return ret;
	}	
	
	//////////////////////////////////////////////////////////////////
    
    private static int IntersectionHeader(Entry<String, String> pair1, Entry<String, String> pair2, boolean useTwoMap ) {
        int ret = 0;        
    	String H1 = SchemaMap.get(pair1.getKey());
    	String H2 = "";
    	if (useTwoMap == true) {
    		H2 = SchemaFinal.get(pair2.getKey() );
    		H2 = H2.replace(SC.COLON, SC.DLMT);
    	}else {
    		H2 = SchemaMap.get(pair2.getKey() );	
    	}  	
    	
    	Map <String, String> M1 = CommonUtil.toMap(H1, false);
    	Map <String, String> M2 = CommonUtil.toMap(H2, false);
    	
    	String[] Ft1 = FeatureIndices(M1, M2);
    	String[] Ft2 = FeatureIndices(M2, M1);
    	
    	
    	// there is some common features found
    	if (Ft1[0].length() > 1) {
    		ret = 1;
    		String key = pair1.getKey() + SC.COLON + pair2.getKey();
    		// table.1-feature-nonIntersection + intersection + table.2-feature-nonIntersection 
    		String val = Ft1[1] + SC.COLON + Ft1[0] + SC.COLON + Ft2[1];    		
    		SchemaFinal.put(key, val);  
    		    		
    //		String[] a1 = val.split(DLMT);  		String[] a2 = H1.split(DLMT);    		String[] a3 = H2.split(DLMT);
    //		System.out.println(key + NL + a1.length);    		
    //    	System.out.println(a2.length + NL + a3.length + NL + "............." + NL);      	
    		
    	}
    	return ret;
    }    
     
    /////////////////////////////////////////////////////////////////////////////
    private static String[] FeatureIndices(Map <String, String> M1, Map <String, String> M2) {
    	String[] ret = new String[2];
    	ret[0] = "";
    	ret[1] = "";
    	
    	Iterator<Map.Entry<String, String>> it = M1.entrySet().iterator();		
		while (it.hasNext()) {
	        Entry<String, String> pair = it.next();	       
	      //  System.out.println(pair.getKey());
	        String ky = pair.getKey().toString();
			if (M2.containsKey(ky) == true) {
				ret[0] += SC.DLMT + M1.get(ky).toString(); // intersection				
			} else {
				ret[1] += SC.DLMT + M1.get(ky).toString() ; // ~ intersection
			}
	        //it.remove(); // avoids a ConcurrentModificationException
	    }				
		////////////////
		ret[0] = CommonUtil.CleanFeatureIndicesString(ret[0]); // intersection
		ret[1] = CommonUtil.CleanFeatureIndicesString(ret[1]); // ~ intersection		
		// System.out.println("common:\t" + ret[0] + NL + "UnCommon (f1+f2):\t" + ret[1] + NL + "=========================" );
    	return ret;
    }
    
 
}
