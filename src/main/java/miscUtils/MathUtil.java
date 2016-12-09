package miscUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.math3.distribution.NormalDistribution;

public class MathUtil {

	//////////////////////////////////////////////////////////////////////////////:
    // check wheter it is date or not..
   // covers a lot of date format  
   public boolean isDate(CharSequence date) {
     //05.10.1981 05-10-1981  07-09-2006 23:00:33  2006-09-07 23:01:24 2003-08-30
     // 2003-30-30    some text   // false
     // some regular expression
     String time = "(\\s(([01]?\\d)|(2[0123]))[:](([012345]\\d)|(60))"
         + "[:](([012345]\\d)|(60)))?"; // with a space before, zero or one time

     // no check for leap years 
     // and 31.02.2006 will also be correct
     String day = "(([12]\\d)|(3[01])|(0?[1-9]))"; // 01 up to 31
     String month = "((1[012])|(0\\d))"; // 01 up to 12
     String year = "\\d{4}";

     // define here all date format
     ArrayList<Pattern> patterns = new ArrayList<Pattern>();
     patterns.add(Pattern.compile(day + "[-.]" + month + "[-.]" + year + time));
     patterns.add(Pattern.compile(year + "-" + month + "-" + day + time));
     // here you can add more date formats if you want

     // check dates
     for (Pattern p : patterns)
       if (p.matcher(date).matches())
         return true;

     return false;

   }
   
   //////////////////////////////////////////////////////////////////////////////:
   // detects the hr:min:sec.millisecond
   public boolean isTime(CharSequence date) {
     //14:47:43.8216833
     // some regular expression
     String time = "(0?[1-9]|[1][0-9]|2[0123]):(0?[1-9]|[0-5][0-9]|[6][0]):"
             + "(0?[1-9]|[0-5][0-9]|[6][0]).([0-9]+)";

     // define here all date format
     ArrayList<Pattern> patterns = new ArrayList<Pattern>();
     patterns.add(Pattern.compile(time));
     // here you can add more date formats if you want

     // check dates
     for (Pattern p : patterns)
       if (p.matcher(date).matches())
         return true;

     return false;

   }
   
   //////////////////////////////////////////////////////////////////////////////
   // detect double and integer both
   public boolean isNumeric(String str) {
     return str.matches("^-?[0-9]+(\\.[0-9]+)?$");
     // return s.matches("[-+]?\\d*\\.?\\d+");  not test 
     }

   //////////////////////////////////////////////////////////////////////////////
   // detects 3E-9, 0E9 as float
   public boolean isFloat(String str) {
     try {
      Float.parseFloat(str);
      return true;
     }
     catch (NumberFormatException ex) {
      return false;
     }  
   }

   //////////////////////////////////////////////////////////////////////////////
	public double calculateProbablity(int featureID,String value,String label, Map<String,String> hm, int targetVariable){
	   String classCount,valueCount;
	   classCount = hm.get(targetVariable + SC.US + label.trim());	   	   
	   double ret = 1.0;	   
	   
	   if(classCount == null) {
		//   System.out.print("\t" + ret);
		   return ret;		  
	   }
	   valueCount = hm.get(featureID + SC.US + value + SC.US + label);	   
	   //System.out.print("-" + hm.size());
	   
	   if(valueCount==null) {
		  // System.out.print("-" + ret);
		   return ret;
	   }
	   
	   Double totalCount = 1.0; // default value;
	   try {
		   totalCount = Double.parseDouble (hm.get(Integer.valueOf(targetVariable)));
	   } catch (Exception e) {
		   totalCount = 1.0;
	   }
	   
	   if (totalCount > 0)
		   ret = 1.0 * (Double.parseDouble(classCount) / (totalCount));
		// weights to increase under represented technique
 	   //if (label.contentEquals("1") == true){   ret *= 1.08;   // System.out.print("\t" + ret);  }
	   return ret;
		}
		
	////////////////////////////////////////////////////////////////////	
	public double calculateGaussian(int featureID,String value,String label, Map<String,String> hm){
		Double mean, variance, val, ret = 1.0;
		if (NumberUtils.isNumber(value) == false)
			return  ret;		
		val = Double.parseDouble(value);
		String values = hm.get(featureID + SC.US + label);		
		if(values!=null) {
	      StringTokenizer tokMeanVariance = new StringTokenizer(values,",");
	      mean = Double.parseDouble(tokMeanVariance.nextToken());
	      variance = Double.parseDouble(tokMeanVariance.nextToken());
	      if(variance==0.0)  
	    	  return 1.0;
	      double sd = Math.sqrt(variance);	 
	      NormalDistribution n =  new NormalDistribution(mean, sd);
	      ret = n.density(val);
	      if(ret ==0.0)	    	
	    	  ret = 1.0;
	      //   System.out.print(val + "," + DeviseConfusionMatric.Formate(mean.doubleValue()) + "," +  DeviseConfusionMatric.Formate(variance.doubleValue()) +  " -> "+ ret + "\t");
		}		
		// weights to increase under represented technique
		//if (label.contentEquals("1") == true){ ret *= 1.08;   // System.out.print("\t" + ret);  }
		
		return ret;	
		}	
		
	 ////////////////////////////////////////////////////////////////////////
	 /*
	  * RandomNumber (whole number) generator with a range
	  * input: lower range
	  * input: max range
	  * output: random number (whole number, not floating)
	  */
	 public int randInt(int min, float mx) {
		    Random rand = new Random();
		    int max = (int)mx;
		    max--;
			int randomNum = rand.nextInt((max - min) + 1) + min;
		//	System.out.print("randomNo: " + randomNum + "\t");
			return randomNum;
		}
		
	//////////////////////////////////////////////////////////////////////////////
	public double RoundUp(double inp, int scale) {
		double ret = inp; 
		BigDecimal bd = new BigDecimal(ret);
	    bd = bd.setScale(scale, RoundingMode.HALF_UP);
	    ret =  bd.doubleValue();
	    return ret;
	}
   //////////////////////////////////////////////////////////////////////////////

	//////////////////////////////////////////////////////////////////////////////

   //////////////////////////////////////////////////////////////////////////////


	
	
}
