package fileIO;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.*;

import miscUtils.Config;
import miscUtils.SC;

import org.apache.hadoop.conf.*;

/*
 * SQL terminology, the default value of all = FALSE -> inner join. 
 * all.x = TRUE -> a left (outer) join // we need this case
 * all.y = TRUE -> right (outer) join
 * (all = TRUE -> (full) outer join. *  
 */

public class FileListing {
	static Config Conf = new Config();
	static String rootPath = (String) Conf.prop.getProperty("rootPath");	
	static String folder2 = rootPath + Conf.prop.getProperty("folder2");
	static String SchemaFile = rootPath + "schemaFilePath";	
   //////////////////////////////////////////////////////////////////////////////
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
    public static String ProcessFiles() {
    	File folder = new File(rootPath);    	
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
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out.println("Processed " + fOld );
	}
	

	
	//////////////////////////////////////////////////////////////////////////////
	// move it to one class... 
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
		         // open input stream file for reading purpose.
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
	public static Map <String,String> PopulateSchemaMap()  {
		
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
            //  System.out.println(Line + "\n");            
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
	/////////////////////////////////////////////////////////////////////////////
	
	
	
}
