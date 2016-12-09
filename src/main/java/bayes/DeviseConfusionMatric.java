package bayes;

import java.text.DecimalFormat;
import java.util.*;

import miscUtils.SC;


public class DeviseConfusionMatric
{	
	private final static long zero = 0;
	private final static double hundred = 100.0;
	
   ///////////////////////////////////////////////////////////////////
	/*
	 * input Map structure
	 * key -> A_B   value -> 45
	 * key -> B_C   value -> 51
	 */
   public static String Draw(Map<String, Long> map) {
	  // iterate through all of the actual and identified classes
	   int i,j, sz;
	   String Ret = SC.ZeroSpace;
	   String ky, line = SC.ZeroSpace;
	   Long count;
	   Double Denominator = 1.0; // ensure non zero denominaotr
	   Double Den = 1.0;
	   
       for (Map.Entry<String, Long> entry : map.entrySet()) {
           ky = entry.getKey().toString();
           line += ky.trim() + SC.DLMT;
           //count = entry.getValue();           
       }  
       line = line.replace(SC.US, SC.DLMT);       
       line = line.replace(SC.SPACE, SC.ZeroSpace);
       
       String[] words = line.split(SC.DLMT);
       Set<String> uniqueWords = new HashSet<String>();
       for (String word : words) {
           uniqueWords.add(word);
       } 
       /*
       System.out.println(line);
       System.out.println(uniqueWords);       
       System.out.println(uniqueWords.size());
       */
       line = uniqueWords.toString();
       line = line.replace(SC.MiddleBracketS, SC.ZeroSpace);
       line = line.replace(SC.MiddleBracketE, SC.ZeroSpace);
       line = line.replace(SC.SPACE, SC.ZeroSpace);
       
       String[] Arr = line.split(SC.DLMT);       
       sz = Arr.length;
       
       for (i=0; i<sz; i++) {
    	   Ret += SC.TB + Arr[i];
       }
       Ret += SC.NL;            
       
       Long TOTAL = zero;
       Long[] TP = new Long[sz];
       Long[] TN = new Long[sz];
       Long[] FP = new Long[sz];
       Long[] FN = new Long[sz];
       Long ACC = zero;
       Long ERR = zero;
        
       
       TP = Initialize (TP, sz);
       TN = Initialize (TN, sz);
       FP = Initialize (FP, sz);
       FN = Initialize (FN, sz);
              
       for (i=0; i<sz; i++) {
    	   Ret += Arr[i] + SC.TB;
    	   
    	   for (j=0; j<sz; j++) {
        	   ky = Arr[i] + SC.US + Arr[j];
        	   count = map.get(ky);
        	   if (count == null) {
        		   count = zero;
        	   }        		   
        	   Ret += count.toString() + SC.TB;
        	           	   
        	   if (i==j) {
        		TP[i] = count;
        		ACC += count;
        	   }
        	   TOTAL += count;
           }   
    	   Ret += SC.NL;
       }  
              
       Ret += SC.NL + "TP" + SC.TB;
       for (i=0; i<TP.length; i++) {
    	   Ret += TP[i] + SC.TB;
       }
       
       TN = Find_TN (TN, sz, map, Arr);
       FN = Find_FN (FN, sz, map, Arr);
       FP = Find_FP (FP, sz, map, Arr);
       
       Ret += SC.NL + "TN" + SC.TB;
       for (i=0; i<sz; i++) {
       	   Ret += TN[i] + SC.TB;
       }
       Ret += SC.NL + "FN" + SC.TB;
       for (i=0; i<sz; i++) {
    	   Ret += FN[i] + SC.TB;
       }
       Ret += SC.NL + "FP" + SC.TB;
       for (i=0; i<sz; i++) {
    	   Ret += FP[i] + SC.TB;
       }
       
       double acc = hundred * ACC / TOTAL;
       double err = hundred - acc;
       ERR = TOTAL - ACC; 
       Ret += SC.NL + "Total Instances: " + TOTAL.toString();
       Ret += SC.NL + "Correctly classified instances: " + ACC.toString() + SC.TB + Formate (acc);
       Ret += SC.NL + "Incorrectly classified instances: " + ERR.toString() + SC.TB + Formate (err);

//   	sensitivity = TPR = TP / (TP + FN)
       Double[] TPR = new Double[sz];       
       Ret += SC.NL + "Sensitivity (TPR):";
       for (i=0; i<sz; i++) {
    	   Den = 1.0 * (TP[i] + FN[i]);
    	   Denominator = (Den == 0.0) ? 1.0 : Den ;
    	   TPR[i] = hundred * TP[i] / Denominator  ;
    	   Ret += SC.TB + Formate (TPR[i]);
       }     	
       
//     Specificity (SPC) = TNR = TN / (FP + TN)
       Double[] TNR = new Double[sz];       
       Ret += SC.NL + "Specificity (SPC):";
       for (i=0; i<sz; i++) {
    	   Den = 1.0 * (FP[i] + TN[i]);
    	   Denominator = (Den == 0.0) ? 1.0 : Den ;    	   
    	   TNR[i] = hundred * TN[i] / Denominator ;
    	   Ret += SC.TB + Formate (TNR[i]);
       }     	

//     Precision = Positive Predictive Value (PPV) = TP / TP + FP
       Double[] PPV = new Double[sz];       
       Ret += SC.NL + "Precision = Positive Predictive Value (PPV):";
       for (i=0; i<sz; i++) {
    	   Den = 1.0 * (TP[i] + FP[i]);  
    	   Denominator = (Den == 0.0) ? 1.0 : Den;
    	   PPV[i] = hundred * TP[i] / Denominator;
    	   Ret += SC.TB + Formate (PPV[i]);
       }     	
	    
//     Negative Predictive Value (NPV) = TN / TN + FN
       Double[] NPV = new Double[sz];       
       Ret += SC.NL + "Negative Predictive Value (NPV):";
       for (i=0; i<sz; i++) {
    	   Den = 1.0 * (TN[i] + FN[i]); 
    	   Denominator = (Den == 0.0) ? 1.0 : Den;    	   
    	   NPV[i] = hundred * TN[i] / Denominator;
    	   Ret += SC.TB + Formate (NPV[i]);
       }     	
	    
       
//     Fall-Out or False Positive Rate (FPR) =  FP / N = FP / FP + TN
       Double[] FPR = new Double[sz];       
       Ret += SC.NL + "Fall-Out / False Positive Rate (FPR):";
       for (i=0; i<sz; i++) {
    	   Den = 1.0 * (FP[i] + TN[i]);
    	   Denominator = (Den == 0.0) ? 1.0 : Den;
    	   FPR[i] = hundred * FP[i] / Denominator  ;
    	   Ret += SC.TB + Formate (FPR[i]);
       }     	
	    
//     False Discovery Rate (FDR) = FDR = FP / FP + TP = 1 - PPV
       Double[] FDR = new Double[sz];       
       Ret += SC.NL + "False Discovery Rate (FDR):";
       for (i=0; i<sz; i++) {
    	   Den = 1.0 * (FP[i] + TP[i]);
    	   Denominator = (Den == 0.0) ? 1.0 : Den;
    	   FDR[i] = hundred * FP[i] / Denominator  ;
    	   Ret += SC.TB + Formate (FDR[i]);
       }     	
 
//	   False Negative Rate (FNR) = FNR = FN / FN + TP = 1 - TPR
       Double[] FNR = new Double[sz];       
       Ret += SC.NL + "False Negative Rate (FNR):";
       for (i=0; i<sz; i++) {
    	   Den = 1.0 * (FN[i] + TP[i]);
    	   Denominator = (Den == 0.0) ? 1.0 : Den;
    	   FNR[i] = hundred * FN[i] / Denominator  ;    
    	   Ret += SC.TB + Formate (FNR[i]);
       }     	

       
//	   F1 = 2TP  / (2TP + FP + FN)
       Double[] F1 = new Double[sz];       
       Ret += SC.NL + "F1 Score:";
       for (i=0; i<sz; i++) {
    	   Den = 1.0 *  (2.0 * TP[i] + FP[i] + FN[i]);
    	   Denominator = (Den == 0.0) ? 1.0 : Den;    	   
    	   F1[i] = hundred * 2.0 * TP[i] / Denominator;
    	   Ret += SC.TB + Formate (F1[i]);
       }     	
	    
       // Matthews correlation coefficient (MCC) = 
       // ((TP*TN)-(FP*FN)) / (sqrt((TP+FP)*(TP+FN)*(TN+FP)*(TN+FN)))  
       Double[] MCC = new Double[sz];       
       Ret += SC.NL + "Matthews correlation coefficient (MCC)";
       for (i=0; i<sz; i++) {    	       	   
    	   Double Numerator = 1.0 * TP[i]  * TN[i]; 
    			  Numerator -= (FP[i] * FN[i]);
    	   Den = 1.0 * (TP[i]+FP[i]) * ( TP[i] + FN[i] ) * ( TN[i] + FP[i] ) * ( TN[i] + FN[i]);
    	   Denominator = (Den == 0.0) ? 1.0 : Den ;    	   
    	    	   Denominator = Math.sqrt(Denominator);
    	   MCC[i] = hundred * Numerator / Denominator;
    	   Ret += SC.TB + Formate (MCC[i]);
       }  
       
       return Ret;
   }
  
   
   ///////////////////////////////////////////////////////////////////
   private static Long[] Find_TN (Long[] v, int sz, Map<String, Long> h, String[] Arr) {
	   int i=0, j=0, marker =0;
	   String ky = ""; Long count;
	   
	   for (marker=0; marker<sz; marker++) {	  
	   			
		   for (i=0; i<sz; i++) {
			   for (j=0; j<sz; j++) {
		      	   ky = Arr[i].trim() + SC.US + Arr[j].trim();
	        	   count = h.get(ky);
	        	   if (count == null) {
	        		   count = zero;
	        	   }      
	        	   if ((i != marker ) && (j != marker )) {
	        		v[marker] += count;
	        	   }	        	   
			   }				   
		   }
	   }
	   return v;
   }

   ///////////////////////////////////////////////////////////////////
   private static Long[] Find_FN (Long[] v, int sz, Map<String, Long> h, String[] Arr) {
	   int i=0, j=0;
	   String ky = ""; Long count;
	   for (i=0; i<sz; i++) {
		   for (j=0; j<sz; j++) {
	      	   ky = Arr[i].trim() + SC.US + Arr[j].trim();
        	   count = h.get(ky);
        	   if (count == null) {
        		   count = zero;
        	   }      
        	   if (i != j)  {
        		v[i] += count;
        	   }
		   }				   
	   }	   
	   return v;
   }   

   ///////////////////////////////////////////////////////////////////
   private static Long[] Find_FP (Long[] v, int sz, Map<String, Long> h, String[] Arr) {
	   int i=0, j=0;
	   String ky = ""; Long count;
	   for (i=0; i<sz; i++) {
		   for (j=0; j<sz; j++) {
	      	   ky = Arr[i].trim() + "_" + Arr[j].trim();
        	   count = h.get(ky);
        	   if (count == null) {
        		   count = zero;
        	   }      
        	   if (i != j)  {
        		v[j] += count;
        	   }
		   }				   
	   }	   
	   return v;
   }   
   
   
///////////////////////////////////////////////////////////////////
	private static Long[] Initialize (Long[] v, int sz) {
		for (int i=0; i<sz; i++) {
			v[i] = zero;
			}
		return v;
	}
///////////////////////////////////////////////////////////////////
	public static void Print_Screen (String ConsoleData) {
		System.out.println(SC.NL + "========================================");
		System.out.print(ConsoleData);
		System.out.println(SC.NL + "========================================");
	}
///////////////////////////////////////////////////////////////////
	public static String Formate (double d) {
		String pattern = "##.##";
		DecimalFormat decimalFormat = new DecimalFormat(pattern);
		String ret = decimalFormat.format(d);
		return ret + "%";
	}
///////////////////////////////////////////////////////////////////

		
}
