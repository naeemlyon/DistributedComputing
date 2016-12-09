package miscUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;



import fileIO.IO;

public class subTitles {
   private static IO io = new IO();
   private static String folder = "/home/mnaeem/sub/";   
//////////////////////////////////////////////////////////////////////////	
	
   public static void main(String[] args) {
	   
	   String fn = "M.srt";	   		
	   String out = ProcessSubTitle(folder + fn);
	   io.WriteFiles(out, folder +  fn.replace(".srt",".out.srt"));	   
   }
	
//////////////////////////////////////////////////////////////////////////	   
   private static String ProcessSubTitle(String fName) {
	   String line = "";
	   String out = "";
	   try {
		   		   
			File file = new File(fName);
			BufferedReader br = new BufferedReader(new InputStreamReader(
                    								new FileInputStream(file), "ISO-8859-15"));
			
			while ((line = br.readLine()) != null) {
				
			if (line.length() > 1) {
			    //line = line.trim();
			    
			    try {
			    	String s = String.valueOf(line.charAt(0));
				    Integer.parseInt(s);	
			    } catch (NumberFormatException en) {
			    	System.out.println(line);
			    	out += line + SC.NL;
			    }
			}
			}
		   br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	   
	   return out;
   }
///////////////////////////////////////////////////////////////////////////
	
}
