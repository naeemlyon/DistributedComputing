package bayes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.commons.math3.distribution.NormalDistribution;

import fileIO.*;
import miscUtils.MathUtil;

public class GenBash
{	
	private static IO io = new IO();
	private static MathUtil MU = new MathUtil();
	private static String bashOutputFolder = "/home/mnaeem/workspace/";
    private static String dataFolder = "/home/mnaeem/workspace/data/sampleFolder/";
	
	//////////////////////////////////////////////////////////////////
    public static void main(String[] args) {
    //	Map<String, Long> h =  DeviceHashMapStringKey();
    //	String out = DeviseConfusionMatric.ConfusionMatrix(h);
    //	DeviseConfusionMatric.Print_Screen(out);
    // 	System.out.println(DistAnalysis (41.9, 0.0, 0.01));
    //  bashOutputFolder = "data/";	 	dataFolder = "data/";
		boolean Header = false; // if first line header, omit it (set True)
		
//		Formulate_Command(dataFolder + "FileToSample", Header, -1);   
	  	
//		Formulate_Command(dataFolder + "OverSample", Header, -1); //   
		Formulate_Command(dataFolder + "iris", Header, -1); // 96%  
//		Formulate_Command(dataFolder + "caterpillartube.data", Header, 6);   
	  	
//		Formulate_Command(dataFolder + "bank", Header, -1); 
//		Formulate_Command(dataFolder + "kddcup", Header, -1);
//    	Formulate_Command(dataFolder + "kddcup99.data", Header, -1);// 87.26% accuracy
//   	Formulate_Command(dataFolder + "input.data", Header, -1); // 100% accuracy
//     	Formulate_Command(dataFolder + "arrhythmia.data", Header, -1); // 67.26%    	 
// 		Formulate_Command(dataFolder + "wallmart.data", Header, -1); // 0% accuracy (only one record correctly out 0.46million records
//     	Formulate_Command(dataFolder + "robots.data", Header, -1);  // 94.88% accuaracy (skewed classes)
//	 	Formulate_Command(dataFolder + "westnile.data",Header,-1);  // 75.76% accuaracy (skewed classes)
// 		Formulate_Command(dataFolder + "ionosphere.data",Header,-1);  // 82.62% accuaracy
    	System.out.println("Scripts created at: '" + bashOutputFolder + "'");
	     	   	
     	       	    	
    }
        
    @SuppressWarnings("unused")
	private static double DistAnalysis (double val, double mean, double sd ) {
    	NormalDistribution n =  new NormalDistribution(mean, sd);
	    double prob = n.density(val);
	    return prob;    	
    }
  
    
   /////////////////////////////////////////////////////////////////// 
   @SuppressWarnings("unused")
   private static Map<String, Long> DeviceHashMapStringKey() {
	   Map<String, Long> m = new HashMap<String, Long>();
	   
	   String[] s = populate();
        for (String a : s) {
            Long freq = m.get(a);
            m.put(a, (freq == null) ? 1 : freq + 1);
        }
 //       System.out.println(m.size() + " distinct words:");
 //       System.out.println(m);
       return m;
   }
    
   private static String[] populate () {
	   String[] s = new String[28];
	   s[0] = "A_B";	s[1] = "B_C";       s[2] = "A_B"; 	 s[3] = "A_B";  
       s[4] = "B_C";    s[5] = "A_C";       s[6] = "C_B";    s[7] = "C_C";
       s[8] = "A_A";    s[9] = "A_C";       s[10] = "C_C";   s[11] = "C_A";
       s[12] = "A_B";   s[13] = "B_C";      s[14] = "B_B";   s[15] = "C_B";
       s[16] = "A_C";   s[17] = "C_A";      s[18] = "A_B";   s[19] = "B_C";
       s[20] = "A_A";   s[21] = "C_C";      s[22] = "B_B";   s[23] = "C_C";
       s[24] = "A_A";   s[25] = "A_A";      s[26] = "A_A";   s[27] = "A_A"; 
       return s;
   }   
   ///////////////////////////////////////////////////////////////////
 
   public void produceAlphabetAndNumbersForColumnNames() {
	      char[] alphabet = "abcdefghijklmnopqrstuvwxyz".toCharArray();
	      int i,j;	      
	      int count = 1;	      
	      String S = "";
	      int sz = alphabet.length;	      
	      for (i=0;  i<sz; i++) {
	          for (j=0; j<sz-1; j++) {              
	              //S += alphabet[i] + "" + alphabet[j] + "," ;
	                  S += count +"," ;	                  
	                 count++;
	                  if (count == 278 ) {                      
	                     sz = 26;
	                  }      
	      }
	      }
	    System.out.print(S);
	    }
   
   /////////////////////////////////////////////////////////////////////////
   public static void Formulate_Command(String fName, boolean Header, int classIndx) {
	   String line = "";
	   int i=0, j=0, numColumns =0;
	   Long  count;
	   String[] Arr;
	   String continousVariables = "";
	   String discreteVariables = "";
	   HashMap<String, Long> h = new HashMap<String, Long>();
	   // output folder for bash files (test.sh, train.sh etc)
	   
	   // take only file name with extension ignoring local path
	   Arr = fName.split("/"); 
	   String fn = Arr[Arr.length-1];
	   String ret= "";
	   
	   String train = "hadoop jar mn.jar bayes.NBTrainDriver -D delimiter=\",\"";
	   train += " -D input=\"" + fn + "\" -D output=\"model\" ";
	   
	   String evaluation = "hadoop jar mn.jar bayes.NBModelEvaluationDriver -D delimiter=\",\"";
	   evaluation += " -D input=\"" + fn + "\" -D output=\"evalresult\" ";
	   	   
	   ///////////////////////////////////////
	   try {
			File file = new File(fName);
			BufferedReader br = new BufferedReader(new FileReader(file));		
		    if (Header)
		    	line = br.readLine(); // omit this header line
			
			line = br.readLine();
		    
		    Arr = line.split(",");
		    numColumns = Arr.length;
		    // class index is 0 based. if las then provide -1
		    if (classIndx == -1)
		    	classIndx = (Arr.length-1);
		
		    // collecting unique class states (for evaluation and test of the model)
		    count = h.get(Arr[classIndx].trim());
		    h.put(Arr[classIndx].trim(), (count == null) ? 1 : count + 1);
		    
		    // building up the continuous or discrete variables index list
		    for (i=0; i<Arr.length; i++) {
		    	if (i != classIndx) {
			    	if ((MU.isNumeric(Arr[i]) == true) || (MU.isFloat(Arr[i]) == true)) {
			    		j = i+1;
			    		continousVariables +=  "," + j;
			    	}
			    	else {
			    		j = i+1;
			    		discreteVariables += "," + j;
			    	}
		    	}
		    }
		    
		    // remove first unwanted comma 
		    if (discreteVariables.length() > 0) 
		    	discreteVariables = discreteVariables.substring(1, discreteVariables.length());
		    if (continousVariables.length() > 0)
		    	continousVariables = continousVariables.substring(1, continousVariables.length());		    			
	        ////////////////////////////////////////////////////////////////
	   
		 // extracting class states (for evaluation and test of the model) 
	       while ((line = br.readLine()) != null) {
	    	    Arr = line.split(",");
			    count = h.get(Arr[classIndx].trim());
			    //System.out.println(h.size());			    
			    h.put(Arr[classIndx].trim(), (count == null) ? 1 : count + 1);
	       } // end of the while 		    			    	
	       br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
   	
	   // these are common data for all three bash files
	   ret += "-D continousVariables=\"" + continousVariables + "\" ";
	   ret += "-D discreteVariables=\"" + discreteVariables + "\" ";
	   ret += "-D targetVariable=\"" + (classIndx +1) + "\" ";
	   ret += " -D numColumns=\"" + numColumns + "\" ";   
	   
	   // train.sh (run on command line. (bash ./train.sh)
	   train += ret;	   
	   io.WriteFiles(train, bashOutputFolder + "train.sh");
	   
	   // evaluation.sh (run on command line. (bash ./evaluation.sh)
	   line = "";
	   for (Map.Entry<String, Long> entry : h.entrySet()) {
           line += entry.getKey().toString() + ",";           
       }  
	   line = line.substring(0, line.length()-1);	
	   evaluation +=ret;
	   evaluation += "-D modelPath=\"model\" -D targetClasses=\"" + line + "\"";
	   io.WriteFiles(evaluation, bashOutputFolder + "evaluation.sh");
	   
	   // test.sh (run on command line. (bash ./test.sh)
	   String test =  evaluation.replace("NBModelEvaluationDriver", "NBTestDriver");
	   test = test.replace("evalresult", "testresult");	   
	   io.WriteFiles(test, bashOutputFolder + "test.sh");
	   
	   ret = "rm result/model\n";
	   ret += "rm result/evalresult\n";
	   ret += "rm result/testresult\n";
	   
	   // hadoop fs -get out/part-r-00000 aggregated.csv
	   ret += "hadoop fs -get model/part-00000 result/model\n";
	   ret += "hadoop fs -get evalresult/part-00000 result/evalresult\n";
	   ret += "hadoop fs -get testresult/part-00000 result/testresult\n";
	   io.WriteFiles(ret, bashOutputFolder + "getresult.sh");
	   
	   line = train + "\n\n" + evaluation + "\n\n" + test + "\n\n" + ret;
	   
	   io.WriteFiles(line, bashOutputFolder + "all.sh");
	   
	   // System.out.println(train);	   System.out.println(evaluation); 	   System.out.println(test);
   }   

   //////////////////////////////////////////////////////////////////////////////:	
 
}
