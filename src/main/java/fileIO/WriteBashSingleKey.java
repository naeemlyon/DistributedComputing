package fileIO;

import java.util.*;
import java.util.Map.Entry;
import com.google.common.collect.Lists;

import miscUtils.*;


public class WriteBashSingleKey {	
	
	static Config Conf = new Config();	
	static String rootPath = (String) Conf.prop.getProperty("rootPath");		
	static String folder2 = rootPath + Conf.prop.getProperty("folder2");
	static String baseFile = (String) Conf.prop.getProperty("baseFile");
	static String joinType = (String) Conf.prop.getProperty("joinType"); 
	static String bashfolder = (String) Conf.prop.getProperty("bashfolder");		
	static Map <String,String> SM = new HashMap <String, String>();
	// // schema in file for all output
	static Map <String,String> Schema = new HashMap <String, String>(); 
	static String schema = bashfolder + "schema.csv"; 
	
	static Integer KeysCount = 1;
	static boolean firstTime = true;
		
	//////////////////////////////////////////////////////////////////////////////
	public static String Run(Map <String,String> sm) {
		
		String ret = "";
		SM.putAll(sm);
		String base = SM.get(baseFile); 		
		ret =RestrictedMaps(base);				
		return ret;
	}
	
	//////////////////////////////////////////////////////////////////////////////
	private static String RestrictedMaps(String base) {
		String ret = " Done!";
		int fileNum = 1;
		boolean contLoop = true;
	
		while (contLoop) {
		 contLoop = false;	
		 Iterator <Map.Entry<String, String>> it = SM.entrySet().iterator();  
		 while (it.hasNext()) {
		  Entry <String, String> p = it.next();
		  List<List<String>> res = intersectData(base, p.getValue());
		  Integer counter = res.get(0).size();
		  if (counter == KeysCount) {
			  contLoop = true;			  
			  WriteAllBash(fileNum, res, p.getKey()); 
			  PopulateSchemaMap(fileNum, res);
			  base = AddElements(res.get(4),res.get(6),res.get(7));
			  Display(res);  
			  fileNum++;
		  }
		 }
		}	
		// scheme.csv to see what is the label ordering in produced csv
		WriteSchema();
		return ret;
	}
	
	//////////////////////////////////////////////////////////////////////////////
	private static List<List<String>> intersectData(String a, String b) {
		List<String> iCM1 = new ArrayList<String>(); //0- indices of common features 1
		List<String> iCM2 = new ArrayList<String>(); //1- indices of common features 2
		List<String> iUC1 = new ArrayList<String>(); //2- indices of uncommon features 1
		List<String> iUC2 = new ArrayList<String>(); //3- indices of uncommon features 2
		List<String> sCM1 = new ArrayList<String>(); //4- value of common features 1
		List<String> sCM2 = new ArrayList<String>(); //5- value of common features 2
		List<String> sUC1 = new ArrayList<String>(); //6- value of uncommon features 1
		List<String> sUC2 = new ArrayList<String>(); //7- value of uncommon features 2
		
		int i=0, j=0;
		String[] arrA = a.split(SC.DLMT);
		String[] arrB = b.split(SC.DLMT);
		
		// common attributes indices
		for (i=0; i< arrA.length; i++) {
			iUC1.add(String.valueOf(i));
	  		sUC1.add(arrA[i].trim());
	  	}
			
		// common attributes indices
  		for (j=0; j< arrB.length; j++) {
  	  		iUC2.add(String.valueOf(j));
  	  		sUC2.add(arrB[j].trim());				  	
  		}
  		
		// common attributes indices
		for (i=0; i< arrA.length; i++) {
			for (j=0; j< arrB.length; j++) {
				String f = arrA[i].trim();
				String s = arrB[j].trim();
			
				if (f.equals(s)) {
			  		// common attributes indices
				//	System.out.println(f + ":" + s);
					
			  		if (!iCM1.contains(String.valueOf(i)))
			  		 	iCM1.add(String.valueOf(i));
			  		if (!iCM2.contains(String.valueOf(j)))
			  			iCM2.add(String.valueOf(j));
			  		if (!sCM1.contains(arrA[i]))
			  			sCM1.add(arrA[i]);
			  		if (!sCM2.contains(arrB[j]))
			  			sCM2.add(arrB[j]);
			  	}
				
			  	
			}					
		}

		sUC1 = RemoveCommonElements(sCM1, sUC1);
		iUC1 = RemoveCommonElements(iCM1, iUC1);
		sUC2 = RemoveCommonElements(sCM2, sUC2);
		iUC2 = RemoveCommonElements(iCM2, iUC2);
			
		List<List<String>> ret = Lists.newArrayList();
		ret.add(iCM1);
		ret.add(iCM2);
		ret.add(iUC1);
		ret.add(iUC2);
		ret.add(sCM1);
		ret.add(sCM2);
		ret.add(sUC1);
		ret.add(sUC2);
		return ret;
	}
	
	
	//////////////////////////////////////////////////////////////////////////////
	private static List<String> RemoveCommonElements(List<String> list, List<String> from ) {
		// remove common items
				Iterator <String> it = list.iterator();
				while (it.hasNext()) {
					from.remove(it.next());
				}
		return from;
	}
	
	//////////////////////////////////////////////////////////////////////////////
	private static String AddElements(List<String> cm, List<String> u1, List<String> u2) {
		String ret = "";
		List <String> list = new ArrayList<String>();
		list.addAll(cm);
		list.addAll(u1);
		list.addAll(u2);
		ret = list.toString();
		ret = ret.replace(SC.MiddleBracketS, SC.ZeroSpace);
		ret = ret.replace(SC.MiddleBracketE, SC.ZeroSpace);
		ret = ret.replace(SC.SPACE, SC.ZeroSpace);
		return ret;
	}
	//////////////////////////////////////////////////////////////////////////////
	/*
	 * 
	 */
	//////////////////////////////////////////////////////////////////////////////
	private static void Display(List<List<String>> res ) {
		  String tmp = res.get(0).size() + ":" +  res.get(1).size() + ":" ;
		  tmp += res.get(2).size() + ":" +  res.get(3).size() + ":" ;
		  tmp += res.get(4).size() + ":" +  res.get(5).size() + ":" ;
		  tmp += res.get(6).size() + ":" +  res.get(7).size() ;
		  //tmp = res.get(6).toString();
		   System.out.println(tmp);

	}
	
	/////////////////////////////////////////////////////////////////////////////	
	/*
	* clear
	* hadoop jar mn.jar reducejoin.JoinDriver -D input="train.csv hdfs://localhost:54310/user/mnaeem/data2/comp_sleeve.csv" 
	* -D KeysA="6" -D ValA="0,1,2,3,4,5,7" 
	* -D KeysB="3" -D ValB="0,1,2,4,5,6,7,8,9" 
	* -D output="out" -D joinType="leftouter" -D DLMT="," 
	* hadoop fs -rmr train.csv
	* hadoop fs -mv out/part-r-00000 train.csv
	* hadoop fs -get train.csv 2.csv
	*/
	////////////////////////////////////////////////////////////////////////////
	private static String WriteAllBash(int fileNum, List<List<String>> inp, String file2 ) {
	
		String ret = "";		
		String rider = "clear" + SC.NL + SC.NL;
		// rider += "rm -r out" + NL + NL;  
		rider += "hadoop jar mn.jar reducejoin.JoinDriver" + SC.SPACE;
		
		String param = "-D input=";
		if (firstTime) {
			param += SC.QUOTE + folder2 +  baseFile +  SC.SPACE + folder2 + file2 + SC.QUOTE + SC.SPACE;
			firstTime=false;		
		}else {
			param += SC.QUOTE + baseFile +  SC.SPACE + folder2 + file2 + SC.QUOTE + SC.SPACE;
		}
		
		String cm1 = PrepareInput("-D KeysA=", inp.get(0).toString());
		String cm2 = PrepareInput("-D KeysB=", inp.get(1).toString());
		String uc1 = PrepareInput("-D ValA=", inp.get(2).toString());
		String uc2 = PrepareInput("-D ValB=", inp.get(3).toString());
		
		param += cm1 + uc1 + cm2 +uc2;
				
		param += "-D output=" + SC.QUOTE + "out" + SC.QUOTE + SC.SPACE;			
		param += "-D joinType=" + SC.QUOTE + joinType + SC.QUOTE + SC.SPACE;
		param += "-D DLMT=" + SC.QUOTE + SC.DLMT + SC.QUOTE + SC.SPACE;			
					
		param += SC.NL + SC.NL;			
		param += "hadoop fs -rmr" + SC.SPACE + baseFile + SC.NL + SC.NL;
		param += "hadoop fs -mv out/part-r-00000" + SC.SPACE + baseFile + SC.NL + SC.NL;
		
		param += "hadoop fs -get" + SC.SPACE + baseFile + SC.SPACE + fileNum + ".csv" + SC.NL;
		
		System.out.println("Writing script numnber: " + fileNum);			
		CommonUtil.WriteFiles(rider + param, bashfolder + fileNum + ".sh");			
		
		ret = "Scripts created at: '" + bashfolder + "'";
		return ret;
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	/*
	* -D KeysA="6" -D ValA="0,1,2,3,4,5,7" 
	* -D KeysB="3" -D ValB="0,1,2,4,5,6,7,8,9" 
	*/	
	//////////////////////////////////////////////////////////////////////////////////
	private static String PrepareInput(String var, String val) {
		String ret = var + SC.QUOTE;
		ret += val.replace(SC.SPACE, SC.ZeroSpace);
		ret += SC.QUOTE + SC.SPACE ;
		ret = ret.replace(SC.MiddleBracketS, SC.ZeroSpace); 
		ret = ret.replace(SC.MiddleBracketE, SC.ZeroSpace); 
		
		return ret;
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	private static void PopulateSchemaMap(int fileNum, List<List<String>> inp) {
		String val = inp.get(4).toString();
		val += SC.DLMT + inp.get(6).toString();
		val += SC.DLMT + inp.get(7).toString();
		Schema.put( String.valueOf(fileNum) ,val);
	}

	///////////////////////////////////////////////////////////////////////////////////
	private static void WriteSchema() {
		String data = "";
		Iterator <Map.Entry<String,String>> it = Schema.entrySet().iterator();
		while (it.hasNext()) {
			Entry <String, String> pair = it.next();
			data += pair.getKey() + SC.DLMT + pair.getValue() + SC.NL;
		}		
		data = data.replace(SC.SPACE ,SC.ZeroSpace);
		data = data.replace(SC.MiddleBracketS, SC.ZeroSpace); 
		data = data.replace(SC.MiddleBracketE, SC.ZeroSpace); 
		
		CommonUtil.WriteFiles(data, schema);
	}

 
}
