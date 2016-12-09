package fileIO;

import java.util.*;
import java.util.Map.Entry;

import miscUtils.*;

public class WriteBashMultipleKeys {	
	static Config Conf = new Config();	
	static String rootPath = (String) Conf.prop.getProperty("rootPath");		
	static String folder2 = rootPath + Conf.prop.getProperty("folder2");
	static String joinType = (String) Conf.prop.getProperty("joinType"); 
	static String bashfolder = (String) Conf.prop.getProperty("bashfolder");
	static Map <String,String> SM = new HashMap <String, String>();
	static Map <String,String> SchemaFinal = new HashMap <String, String>();
	static String INTERMEDIATE = (String) Conf.prop.getProperty("intermediate");
	
	//////////////////////////////////////////////////////////////////////////////
	public static String Run(Map <String,String> hMap, Map <String,String> sm) {
		String ret = "";
		
		SchemaFinal.putAll(hMap);		
		SM.putAll(sm);
		ret = writeAllBash();
		return ret;
	}
	
	//////////////////////////////////////////////////////////////////////////////
	// get the largest schema to plug into the final merged file
	private static String getSchemaForWriteup() {
		String ret= "tableCount" + SC.DLMT + "Tables" + SC.DLMT;
		ret += "Features(1-n)" + SC.NL;
		String[] arr = null;
		int sz = 0;
		
		Iterator <Map.Entry<String, String>> t = SchemaFinal.entrySet().iterator();
		while (t.hasNext()) {
			Entry<String, String> p = t.next();
			
			arr = p.getKey().split(SC.COLON);
			sz = arr.length; // number of files
			
			ret += sz + SC.DLMT + p.getKey()+ SC.DLMT;
			
			// CommonFeatures (keys) + unCommonFeatures (File.1) + unCommonFeatures (File.2)
			arr = p.getValue().split(SC.COLON);
			ret += arr[1] + SC.DLMT + arr[0] + SC.DLMT + arr[2] + SC.DLMT + SC.NL ;
			
			// replace two consective DLMT to settle issue when all of the
    		// fetures are in common and nothing is unCommon
    		ret = ret.replace(SC.DLMT + SC.DLMT,SC.DLMT);  
    		
			
		}
		
		return ret;
	}

	/////////////////////////////////////////////////////////////////////////////	
	/*
	 * hadoop fs -get out out
	 */		
	private static String writeAllBash() {
		
		String ret = "";		
		String param = "";
		int fileNum = 0;
		String rider = "clear" + SC.NL + SC.NL;
		// rider += "rm -r out" + NL + NL;  
		rider += "hadoop jar mn.jar reducejoin.JoinDriver" + SC.SPACE;
		
		Iterator <Map.Entry<String,String>> t = SchemaFinal.entrySet().iterator();
		while (t.hasNext()) {
			param = "";			
			Entry<String, String> p = t.next();						
			param += prepareInputBash(p);			
			param += "-D output=" + SC.QUOTE + "out" + SC.QUOTE + SC.SPACE;			
			param += "-D joinType=" + SC.QUOTE + joinType + SC.QUOTE + SC.SPACE;
			param += "-D DLMT=" + SC.QUOTE + SC.DLMT + SC.QUOTE + SC.SPACE;			
			param += prepareOutputBash(p);			
			param += SC.NL + SC.NL;			
			param += "hadoop fs -rmr" + SC.SPACE + INTERMEDIATE + SC.NL + SC.NL;
			param += "hadoop fs -mv out/part-r-00000" + SC.SPACE + INTERMEDIATE + SC.NL + SC.NL;
			
			fileNum = p.getKey().split(SC.COLON).length;
			
			param += "hadoop fs -get" + SC.SPACE + INTERMEDIATE + SC.SPACE + fileNum + ".csv" + SC.NL;
			
			System.out.println("Writing script numnber: " + fileNum);			
			CommonUtil.WriteFiles(rider + param, bashfolder + p.getKey().split(SC.COLON).length + ".sh");			
		}
		
		// get the largest schema to plug into the final merged file
		ret = getSchemaForWriteup();
		CommonUtil.WriteFiles(ret, bashfolder + "finalSchema.csv");
		ret = "Scripts created at: '" + bashfolder + "'";
		return ret;
	}
	/*
	 * -D input="test train"
	 */
	/////////////////////////////////////////////////////////////////////////////
	private static String prepareInputBash(Entry<String, String> p) {		
		String ret = "-D input=";
		String[] arr = p.getKey().split(SC.COLON);
		if (arr.length == 2) {
			ret += SC.QUOTE + folder2 +  arr[0] +  SC.SPACE + folder2 + arr[1] + SC.QUOTE + SC.SPACE;
		} else {
			ret += SC.QUOTE + folder2 +  arr[0] +  SC.SPACE + INTERMEDIATE + SC.QUOTE + SC.SPACE;
		}
		return ret;
	}
	
	/////////////////////////////////////////////////////////////////////////////
	/*
	 * -D KeysA="7" -D ValA="0,1,2,3,4,5,6" -D DLMT="," -D KeysB="6" -D ValB="0,1,2,3,4,5,7"
	 */
	private static String prepareOutputBash(Entry<String, String> p) {		
		String ret = "-D KeysA=" + SM.size();		
		String[] arr = p.getKey().split(SC.COLON);
		
		// file 1
		String File1 = arr[0];
		boolean firstBash = false;
		
		String File2 =  p.getKey().replace(arr[0] + SC.COLON, "");
		int curSz = arr.length;			
		if (curSz == 2) {			
			firstBash = true;
		}
		//System.out.println(File1 + " == " + File2);
		
		
				
		arr = p.getValue().split(SC.COLON);
		// feature 1
		String Ft1 = arr[0];
		// feature 2
		String Fc = arr[1];
		// features common
		String Ft2 = arr[2];		
		
		/*
		if (Ft2.startsWith(DLMT) == true) {
			Ft2 = Ft2.substring(1);
		}
		*/
		// System.out.println("Ft1: " + Ft1 + NL + "Fc:  " + Fc + NL + "Ft2: " + Ft2);
		
		ret = prepareParamBashF1(File1, Ft1, Fc);
		
		if (firstBash) {
			firstBash = false;
			String tmp = prepareParamBashF1(File2, Ft2, Fc);
			tmp = tmp.replace("KeysA", "KeysB");
			tmp = tmp.replace("ValA", "ValB");
			ret += tmp;			
			
		} else {
			ret += prepareParamBashF2(File2, Ft2, Fc);
		}
		
		return ret;
	}
	
	///////////////////////////////////////////////////////////////////////////////////////	
	private static String prepareParamBashF2(String file, String unCommon, String Common) {
		String ret = "";
		int i=0;
		// -D KeysB="3,7" -D ValB="0,1,2,4,5,6"
		String common = "-D KeysB=" + SC.QUOTE;
		String uncommon="-D ValB=" + SC.QUOTE; 
		String[] arr = null;
			
		String data = SchemaFinal.get(file).toString().trim();
		arr = data.split(SC.COLON);
		
		/////////// bug		
		
		data = arr[1] + SC.DLMT + arr[0] + SC.DLMT + arr[2];
		data = data.replace(SC.DLMT + SC.DLMT, SC.DLMT);
		
		
		Map <String, String> M = CommonUtil.toMap(data, true);
				
		arr = Common.split(SC.DLMT);		
		
	//	System.out.print(arr.length + "\t");		
		for (i=0; i<arr.length; i++) {
			common += M.get(arr[i].trim()) + SC.DLMT;
		}
		common = common.substring(0,common.length()-1);
		common += SC.QUOTE;
		
		arr = unCommon.split(SC.DLMT);
	//	System.out.print(arr.length + "\t");
		
		for (i=0; i<arr.length; i++) {
			uncommon += M.get(arr[i].trim()) + SC.DLMT;
		}
		uncommon = uncommon.substring(0,uncommon.length()-1);
		uncommon += SC.QUOTE;
		
		ret = common + SC.SPACE + uncommon + SC.SPACE;
		
		return ret;
	}
	
	///////////////////////////////////////////////////////////////////////////////////////
	
	private static String prepareParamBashF1(String file, String unCommon, String Common) {
		String ret = "";
		int i=0;
		// -D KeysA="3,7" -D ValA="0,1,2,4,5,6"
		String common = "-D KeysA=" + SC.QUOTE;
		String uncommon="-D ValA=" + SC.QUOTE; 
		String[] arr = null;
			
		Map <String, String> M = CommonUtil.toMap(SM.get(file).toString().trim(), true);
				
		arr = Common.split(SC.DLMT);		
		for (i=0; i<arr.length; i++) {
			common += M.get(arr[i].trim()) + SC.DLMT;
		}
		common = common.substring(0,common.length()-1);
		common += SC.QUOTE;
		
		arr = unCommon.split(SC.DLMT);
		for (i=0; i<arr.length; i++) {
			uncommon += M.get(arr[i].trim()) + SC.DLMT;
		}
		uncommon = uncommon.substring(0,uncommon.length()-1);
		uncommon += SC.QUOTE;
		
		ret = common + SC.SPACE + uncommon + SC.SPACE;
		
		return ret;
	}
	///////////////////////////////////////////////////////////////////////////////////////
 
}
