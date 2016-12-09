package reducejoin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.*;

import miscUtils.*;

import org.apache.hadoop.conf.*;

/*
 * SQL terminology, the default value of all = FALSE -> inner join. 
 * all.x = TRUE -> a left (outer) join // we need this case
 * all.y = TRUE -> right (outer) join
 * (all = TRUE -> (full) outer join. *  
 */

public class PrepareBashBinaryTables {
	static Config Conf = new Config();	
	static String rootPath = (String) Conf.prop.getProperty("rootPath");		
	static String folder2 = rootPath + Conf.prop.getProperty("data2");
	static String folder1 =  (String) Conf.prop.getProperty("folder1");
	static String joinType = (String) Conf.prop.getProperty("joinType"); 
	/*
	static String rootPath = "hdfs://localhost:54310/user/mnaeem/";		
	static String folder2 = rootPath + "data2/";
	*/
	// static String bashfolder = "/home/mnaeem/workspace/bashscripts/";
	static String bashfolder = (String) Conf.prop.getProperty("bashfolder");
		
	static String SchemaFile = rootPath  + Conf.prop.getProperty("schemaFile");
	// static String SchemaFile = folder1 + Conf.prop.getProperty("schemaFile");
	
	static Map <String,String> SchemaMap = new HashMap <String, String>();
    
	///////////////////////////////////////////////////////////////////////
	
	private static String BuildAllBash(FileStatus[] FileList) {
		String ret = "";
		int i=0,j=0;
		int sz = FileList.length;
    	System.out.println(sz+ " concepts from hdfs loaded");
    	
    	// one mapHash with all file names
    	
    	
    	
		for (i=0; i<sz-1; i++) {
    		for (j=i+1; j<sz; j++) {
    			System.out.println(FileName(i,j,FileList[i],FileList[j]));
    			ret = Buildindices( FileList[i],FileList[j]);
    	    	ret = Buildindices(FileList[i], FileList[j], ret, i,j);    	    
    		}
    	} 				
		return ret;
	}
	
	
	//////////////////////////////////////////////////////////////////
    public static void main(String[] args) {
    	
    	// only one time and then copy output folder input2 to hdfs	
    	// ret = ProcessFiles();  
    	
    	
    	FileStatus[] FileList = ListAllFiles("input2");
    	SchemaMap = PopulateSchemaMap();
    	
    	BuildAllBash(FileList);
    	
    	
    	/*
    	int sz = FileList.length;
    	System.out.println(sz+ " concepts from hdfs loaded");
    	int i=9,j=10;
    	String ret = "";
    	for (i=0; i<sz-1; i++) {
    		for (j=i+1; j<sz; j++) {
    			System.out.println(FileName(i,j,FileList[i],FileList[j]));
    			ret = Buildindices( FileList[i],FileList[j]);
    	    	ret = Buildindices(FileList[i], FileList[j], ret, i,j);    	    
    		}
    	} 	
    	*/
	    System.out.println("Scripts created at: '" + bashfolder + "'");	    	       
    }
        

    
	///////////////////////////////////////////////////////////////////////
	private static String Buildindices(FileStatus path1, FileStatus path2, String input, int f1, int f2) {
		if (input.length() == 0) 
			return "Nothing in common";
		
		String F1 = FileName(path1);
		String F2 = FileName(path2);		
		String train = "hadoop jar mn.jar reducejoin.JoinDriver input/" + F1;
		train += " input/" + F2 + " out inner ";		
		train += input; 
		WriteFiles(train, bashfolder + "train." + f1 + "." + f2 + ".sh");
		return train;
	}

    /////////////////////////////////////////////////////////////////////////////
    private static String Buildindices(FileStatus path1, FileStatus path2) {
       	String[] Arr = IntersectionHeader(path1, path2);
       	String ret = "1.common" + SC.COLON + SC.SPACE;
       	ret +=  Arr[0] + SC.NL + "1.others" + SC.COLON + SC.SPACE + Arr[1];
       	ret += "\n2.common: " +  Arr[2] + "\n2.others: " + Arr[3] + "\n";       	
       	//System.out.println("\n" + Arr[0].length() + ":" + Arr[1].length() + ":" + Arr[2].length() + ":" + Arr[3].length() + "\n" );
       	//System.out.println(ret);       	
       	
       	// if no common then signal it
       	int mul = Arr[0].length() * Arr[1].length();
       	mul *= Arr[2].length() * Arr[3].length();
       	if (mul == 0){
       		return SC.ZeroSpace;
       	} 
       	ret = Arr[0] + SC.SPACE + Arr[1] + SC.SPACE;
       	ret += SC.DLMT + SC.SPACE + Arr[2] + SC.SPACE + Arr[3];
       	return ret;    	
    }        
    
	/////////////////////////////////////////////////////////////////////////////
    private static String[] IntersectionHeader(FileStatus path1, FileStatus path2) {
        String[] Ft1 = new String[2];
    	String[] Ft2 = new String[2];
    	
    	String H1 = ReadHeader(path1);
    	String H2 = ReadHeader(path2);
    	
    	Map <String, Integer> M1 =  toMap(H1);
    	Map <String, Integer> M2 =  toMap(H2);
    	
    	Ft1 = FeatureIndices(M1, M2);
    	Ft2 = FeatureIndices(M2, M1);
    	String[] ret = new String[4];
    	
    	
    	ret[0] = SortArray (Ft1[0]); // sort common - entity 1
    	ret[1] = SortArray (Ft1[1]); // sort uncommon - entity 1
    	ret[2] = SortLocal (ret[0], H1, H2); // sort common - entity 2 w.r.t - entity 1 common set
    	ret[3] = SortArray (Ft2[1]); // sort uncommon - entity 2
    	
    	return ret;
    }
    
    ///////////////////////////////////////////////////////////////////////////
    
    private static String SortArray(String In) {
    	String ret = "";
    	
    	// for unique number like 0 or 1
    	if (In.trim().length() == 1) 
    		return In.trim();
    	
    	String[] arrS = In.split(SC.DLMT);
    	int sz = arrS.length;
    	int i=0;
    	Long[] arrL = new Long[arrS.length];
    	if (sz > 1) {    		    	
        	for (i=0; i<sz; i++) {
        	   arrL[i] = Long.parseLong(arrS[i]);	
        	}    		
    		java.util.Arrays.sort(arrL);
        	ret = Arrays.toString(arrL);	
    	}
    	ret = ret.replace(SC.MiddleBracketS,SC.ZeroSpace);
    	ret = ret.replace(SC.MiddleBracketE,SC.ZeroSpace);
    	ret = ret.replace(SC.SPACE,SC.ZeroSpace);    	
    	return ret.trim();
    }
    
    ///////////////////////////////////////////////////////////////////////////

    private static String SortLocal(String Ind1, String H1, String H2) {
    	String ret = "";
    	String[] arrH1 = H1.split(SC.DLMT);
    	String[] arrH2 = H2.split(SC.DLMT);
    	String[] arrInd1 = Ind1.split(SC.DLMT);
    	int i= arrH1.length * arrH2.length * Ind1.length() ;
    	// if there is nothing in common the return empty string 
    	if (i==0) {
    		// System.out.println(arrH1.length + ":" + arrH2.length + ":" + Ind1);
    		return ret;
    	}
    	Map<Integer, String> mapH1 = new HashMap<Integer, String>();
    	Map<String, String> mapH2 = new HashMap<String, String>();
    	
    	for (i=0; i<arrH1.length; i++) {
    		mapH1.put(i, arrH1[i]);
    	}
    	
    	for (i=0; i<arrH2.length; i++) {
    		mapH2.put(arrH2[i],  i + SC.ZeroSpace);
    	}
    	
    	// System.out.println("\n" + mapH1 + "\n\n" + mapH2 + "\n\n");
    	
    	for (i=0; i<arrInd1.length; i++) {
    		int c = Integer.parseInt(arrInd1[i]);
    		String head = mapH1.get(c);    		
    		// System.out.println(c + " ----- " + head);    		
    		ret += "," + mapH2.get(head);
    	}
    	
    	ret = ret.substring(1);
    	return ret;
    }
    
    ///////////////////////////////////////////////////////////////////////////
    
    private static String ReadHeader(FileStatus path) {
    	String key = FileName(path);
    	String val = SchemaMap.get(key);
    	return val;
    }
     
    /////////////////////////////////////////////////////////////////////////
    private static Map <String, Integer> toMap(String input) {
    	Map <String, Integer> map = new HashMap <String, Integer>();
    	String[] arr = input.split(SC.DLMT); 
    	int i =0;
    	for (i=0; i<arr.length; i++) {
    		map.put(arr[i].trim(), i);
    	}
    //	System.out.println("\n" + map + "\n");
    	
    	return map;
    }   
    
    /////////////////////////////////////////////////////////////////////////////
    private static String[] FeatureIndices(Map <String, Integer> M1, Map <String, Integer> M2) {
    	String[] ret = new String[2];
    	ret[0] = "";
    	ret[1] = "";
    	
    	Iterator<Map.Entry<String, Integer>> it = M1.entrySet().iterator();		
		while (it.hasNext()) {
	        Entry<String, Integer> pair = it.next();	       
	      //  System.out.println(pair.getKey());
	        String ky = pair.getKey().toString();
			if (M2.containsKey(ky) == true) {
				ret[0] += SC.DLMT + M1.get(ky).toString(); // intersection
			} else {
				ret[1] += SC.DLMT + M1.get(ky).toString() ; // ~ intersection
			}
	       // it.remove(); // avoids a ConcurrentModificationException
	    }
		
		ret[0] = CleanFeatureIndicesString(ret[0]); // intersection
		ret[1] = CleanFeatureIndicesString(ret[1]); // ~ intersection
		
	//	System.out.println(ret[0] + "\n\n" + ret[1] + "\n" );
    	return ret;
    }
    
    ////////////////////////////////////////////////////////////////////////////
    private static String CleanFeatureIndicesString(String input) {
    	String ret = "";
    	if (input.length() > 1){
    		  ret = input.substring(1);
    	}
    	return ret;
    }
    
   ////////////////////////////////////////////////////////////////////////
   private static String FileName(int i, int j, FileStatus F, FileStatus S) {
	   String ret = i + "." + FileName(F) ;
	   ret += "\t" + j + "." + FileName(S);
	   return ret;
   }
   
   ////////////////////////////////////////////////////////////////////////
   private static String FileName(FileStatus F) {
	   String ret = "";
	   String[] arr =  F.getPath().toString().split("/");
	   ret = arr[arr.length-1];
	   return ret;
   }
   ////////////////////////////////////////////////////////////////////////
    
    public static FileStatus[] ListAllFiles(String folder){
    	FileStatus[] status = null;
        try{
                FileSystem fs = FileSystem.get(new Configuration());                
                status = fs.listStatus(new Path(rootPath + folder));
        }catch(Exception e){
                System.out.println(e.getMessage());
        }        
        return status;
    }

    /////////////////////////////////////////////////////////////////////////
    @SuppressWarnings("unused")
	private static String ProcessFiles() {
    	File folder = new File(folder1);    	
      	File[] listOfFiles = folder.listFiles();
          	
    	for (int i = 0; i < listOfFiles.length; i++) {
    	  File file = listOfFiles[i];
    	  if (file.isFile() && file.getName().endsWith(".csv")) {
    	    ProcessFiles(file.getPath(), file.getName() );
    	  } 
    	}
    	return " Done!";
    }    
    
    ////////////////////////////////////////////////////////////////////////
	private static void ProcessFiles(String fOld, String fNew) {
		String  thisLine = null;
		Reader filePath;
		File schemaFile = new File(SchemaFile);
		File filePathNew = new File(folder2 + fNew);
		
		try {
			filePath = new FileReader((fOld));
		      try {
		         // open input stream test.txt for reading purpose.
		         BufferedReader br = new BufferedReader(filePath);
		         // append it to
		         thisLine = fNew + "," + br.readLine() + SC.NL;
		         FileUtils.writeStringToFile(schemaFile, thisLine, true);
		            
		         while ((thisLine = br.readLine()) != null) {
		        	thisLine = thisLine.trim() + SC.NL;		        	
		        	FileUtils.writeStringToFile(filePathNew, thisLine, true);
		         }
		         br.close();
		      }catch(Exception e){
		         e.printStackTrace();
		     }		
		} 
		catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		System.out.println("Processed " + fOld );
	}
	
	///////////////////////////////////////////////////////////////////////
	@SuppressWarnings("unused")
	private static Map <String,String> PopulateSchemaMapLocal() {
		Reader filePath;
		Map <String,String> SMap = new HashMap <String, String>();
		int i = 0;
		String[] arr = null;
		String Line = "";		
		try {
			filePath = new FileReader((SchemaFile));
		      try {
		         // open input stream test.txt for reading purpose.
		         BufferedReader br = new BufferedReader(filePath);
		         while ((Line = br.readLine()) != null) {
		        	Line = Line.trim();
		        	arr = Line.split(SC.DLMT);		        	 
		        	String key = arr[0];
		        	String value = arr[1];
		        	for (i=2; i<arr.length; i++ ) {
		        		value += SC.DLMT + arr[i];
		        	}
		        	SMap.put(key, value);
		         }
		         br.close();
		      }catch(Exception e){
		         e.printStackTrace();
		     }		
		} 
		catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}		
		// System.out.println(SchemaMap.size() + " schemas loaded");
		return SMap;
	}	
	
	/////////////////////////////////////////////////////////////////////////////	
	private static Map <String,String> PopulateSchemaMap()  {
		
		// true -> HDFSMode; false -> false
		boolean HDFSMode = SchemaFile.contains("hdfs:");
		
		Map <String,String> SMap = new HashMap <String, String>();		
		FileStatus[] status = null;				
		BufferedReader br = null;
		try{
            
			if (HDFSMode == true) {
				FileSystem fs = FileSystem.get(new Configuration());                
	            status = fs.listStatus(new Path(SchemaFile));
	        	br=new BufferedReader(new InputStreamReader(fs.open(status[0].getPath())));	
			}
			else {
				Reader filePath = new FileReader((SchemaFile));
			    // open input stream test.txt for reading purpose.
			    br = new BufferedReader(filePath);			    	
			}
			
        	SMap = PopulateSchemaMap(br);
        	br.close();
        	
		} catch (IOException e) {			
			e.printStackTrace();		
		}catch(Exception e){
			e.printStackTrace();
			//System.out.println(e.getMessage());
		}
		
		return SMap;
    }
	///////////////////////////////////////////////////////////////////////////
	
	private static Map <String,String> PopulateSchemaMap(BufferedReader br) throws IOException  {
		Map <String,String> S = new HashMap <String, String>();
		String Line = "";
		int i=0;
		String[] arr = null;
		while ((Line = br.readLine()) != null) {        
        	Line = Line.trim();
            //  System.out.println(Line + SpecialCharacter.NL);            
            arr = Line.split(SC.DLMT);		        	 
        	String key = arr[0];
        	String value = arr[1];
        	for (i=2; i<arr.length; i++ ) {
        		value += SC.DLMT + arr[i];
        	}            	
        	S.put(key, value);
        }		
        System.out.println(S.size() + " schemas loaded");
		return S;
	}

	
	//////////////////////////////////////////////////////////////////////////////
	// move it to one class... it also occurs in schemabuilder
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
	//////////////////////////////////////////////////////////////////////////////

 
}
