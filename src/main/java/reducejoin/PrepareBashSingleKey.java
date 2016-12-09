package reducejoin;

import java.util.*;

import fileIO.*;
import miscUtils.CommonUtil;
import miscUtils.Config;


/*
 * SQL terminology, the default value of all = FALSE -> inner join. 
 * all.x = TRUE -> a left (outer) join // we need this case
 * all.y = TRUE -> right (outer) join
 * (all = TRUE -> (full) outer join. *  
 */

public class PrepareBashSingleKey {	
	// see FileListing.java for detail of the folder input / output
	static Config Conf = new Config();	
	static String schemaFile = (String) Conf.prop.getProperty("schemaFile");
	///////////////////////////////////////////////////////////////////////
	
	public static void main(String[] args) {
		boolean firstTime = false;
		Map <String,String> SchemaMap = new HashMap<String,String>();
	  	
		// only one time and then copy output folder data2 to hdfs
		if (firstTime ) {
			String ret = FileListing.ProcessFiles();	
			System.out.println(ret);
		}
		
	  	SchemaMap = FileListing.PopulateSchemaMap();
	    CommonUtil.Serialize(SchemaMap, schemaFile);
    	//SchemaMap = CommonUtil.Deserialize(schemaFile);
    	 miscUtils.CommonUtil.DisplayMap(SchemaMap);
    	String ret = WriteBashSingleKey.Run(SchemaMap);
    	System.out.println(ret);
    }   
}
