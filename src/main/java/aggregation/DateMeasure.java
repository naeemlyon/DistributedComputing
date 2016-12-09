package aggregation;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import miscUtils.CommonUtil;
import miscUtils.SC;

public class DateMeasure {
	private String DLMT = ",";
	private static int binCount = 4; // by default value	
	private static double logBase = 2.0;	
	private static DateMeasure DM = new DateMeasure();
	private static SimpleDateFormat df = new SimpleDateFormat("dd-MM-yy");
 	      
	////////////////////////////////////////////////////////////////////////////////////////
	public void setDLMT(String val) {
		this.DLMT = val;
	}

	public String getDLMT() {
		return DLMT;
	}
	
	//////////////////////////////////////////////////////////////////////////////////
	private static Map<Integer, String> Discretize(List<Date> arrLst, Date min, Date max) {
		List<Float> arr = new ArrayList<Float>();
		int i =0;
		for (i=0; i<arrLst.size(); i++) {
			arr.add((float) arrLst.get(i).getTime());
		}
		float mn = (float) min.getTime();
		float mx = (float) max.getTime();
		return Discretize(arr, mn, mx);
	}

	
	//////////////////////////////////////////////////////////////////////////////////
	private static Map<Integer, String> Discretize(List<Float> arrLst, float min, float max) {
		Map<Integer, String> Bins = new HashMap<Integer, String>();
		float f =0;
		String s = "";
		int k=0;
		float offSet = (max - min )/ binCount;
		//System.out.println("....offSet:" + offSet);
		int i =0;
		for (i=0; i<arrLst.size(); i++) {
			f = arrLst.get(i);
			k = binCount-1;
			while (k > -1 ) {
				if ( ( f >= (min + (k * offSet))) && 
					( f <= (min + ((k+1) * offSet))) ) {
					s = Bins.containsKey(k) ? Bins.get(k) : "";
					s += SC.DLMT + String.valueOf(f); 
					Bins.put(k, s);
					k = -1; // we found, no need to be in loop
				}
				k--;
			}
		}
		return Bins;
	}
	
	////////////////////////////////////////////////////////////////////////////////////////
	public static void main(String[] args) throws Exception {	
		simulate(1);
		
		String inp = populateTestData();
		String ret = DM.Process (inp);
		ret = ret.replace(SC.DLMT, SC.NL);
		System.out.println(ret);
		
	}


	// calculate standard deviation
	private static float calcStdDev(List<Date> arrVal, float mean) {
		float sumOfSquares = 0.0f;
		float ret = 0; // default value if only one observation
		long count = arrVal.size();
		for (Date v : arrVal) {
			float f = v.getTime() - mean;
			sumOfSquares += (f * f);	
		}
		
		if (count > 1) {
			ret = (float) Math.sqrt(sumOfSquares / (count - 1));	
		}
		return ret; // Standard Devision 
	}
	////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	private static Date[] calcMedianMinMax(List<Date> arrVal, float mean, long count) {
		// 0-Median, 1-Min, 2-Max
		Date[] ret = new Date[3];
		try {
			// sort commentLengths to calculate median, min, max
			Collections.sort(arrVal); // asceding order by default
			// if commentLengths is an even value, average middle two elements
			if (count % 2 == 0) {
				long m1 = arrVal.get(((int) count / 2) - 1).getTime();
				m1 += arrVal.get((int) count / 2).getTime();
				m1 /= 2;
				ret[0] = new Date(m1);
			} else {
				// else, set median to middle value
				ret[0] = arrVal.get((int) count / 2);
			}
			
			ret[1] = arrVal.get(0);               // First element
			ret[2] = arrVal.get(arrVal.size()-1); // Last element		    
		}
		catch (IndexOutOfBoundsException e) {
		//	System.out.println(e.getMessage());
		//	System.out.println("** arrVal.size(): " + arrVal.size() + " **");			
		}			
		return ret;
	}

	////////////////////////////////////////////////////////////////////////////////////////	
	private static float calcGiniCoefficient(List<Date> arrVal, float mean) {
		if (arrVal.size() < 1) return 0;  // c'est introuble
		if (arrVal.size() == 1) return 0; 
		if (mean == 0L) return 0; // means every observation is zero
		
		float ret = 0;
		for (int i = 0; i < arrVal.size(); i++)	{
			for (int j = 0; j < arrVal.size(); j++)	{
				if (i == j) continue;
				ret += (Math.abs(arrVal.get(i).getTime() - 
						arrVal.get(j).getTime()));
			}
		}
		ret = (float) (ret / (2.0 * arrVal.size() * arrVal.size()));
		return (ret / mean); // gini index
	}

	////////////////////////////////////////////////////////////////////////////////////////
	/*
	* marginal	entropy of X , 
	* and its mutual information with itself.	
	*/
	////////////////////////////////////////////////////////////////////////////////////////
	private static float calcEntropy(Map<Integer, String> Bins, long totalObs) {
		float ret = 0.0f;
		float entropy = 0.0f;
		double lg = 0.0;
		
		//System.out.println("totalObs:logBase: " + totalObs + ":" + logBase
		//+ " (Suggested)bincount: " + binCount + " thielMean: " + thielMean);
		//System.out.print("freq:entr:lg -> ");
		
		for (int i=0; i<binCount; i++) {
			if (Bins.containsKey(i)) {
				String[] arr = (Bins.get(i).toString()).split(SC.DLMT);
				int freq= arr.length-1;
			
				entropy = (float) freq/(float) totalObs;
				lg = Math.log(Double.parseDouble(String.valueOf(entropy)))
				/Math.log(logBase);
				//		System.out.print(freq +":" + entropy + ":" + lg + SC.TB);
				if (Double.isInfinite(lg) == false ) {
					ret +=  entropy * (float) lg;			
				}
			}
		}
		return -1 * ret; // // Entropy 
	}

	
	////////////////////////////////////////////////////////////////////////////////////////
	private static float calcTheil(Map<Integer, String> Bins, long totalObs, float globalSum) {
		float ret = 0f;
		float theil = 0.0f;
		double lg = 0.0;
		float globalMean = globalSum / Bins.size();
	//	System.out.println("totalObs:logBase: " + totalObs + ":" + logBase
	//			+ " (Suggested)bincount: " + binCount);
	//	System.out.print("p:x:" + SC.Formate(globalMean) + " -> ");

		for (int i=0; i<binCount; i++) {
			if (Bins.containsKey(i)) {
				String[] arr = (Bins.get(i).toString()).split(SC.DLMT);
				int binObs = arr.length-1;
				float p = (float) binObs / totalObs;
				
				float x = localSum (arr);
				theil = x / globalMean;	
				lg = Math.log(Double.parseDouble(String.valueOf(theil)));
			
		//		System.out.print(SC.Formate(p) +":" + SC.Formate(x) + SC.TB);
				if (Double.isInfinite(lg) == false ) {
					ret +=  p * theil *  (float) lg;	
				}						
			}
		}
		return ret; // Theil 
	}
	
	/////////////////////////////////////////////////////////////////////////////////
	private static float localSum (String[] arr) {
		float sum=0f;
		for (int i=0; i<arr.length; i++){
			if (!CommonUtil.isNullOrBlank(arr[i])) {
				sum += Float.parseFloat(arr[i]);
			}
		}
		return sum;
	}	
	////////////////////////////////////////////////////////////////////////////////////////
	
	public String Process (String inp) {
		String ret = SC.ZeroSpace;
		String[] arr = inp.split(SC.DLMT);
		int i=0;
		float sum = 0;
				
		List<Date> arrLst = new ArrayList<Date>();
		
		try {
			for (i=1; i<arr.length; i++) {
				Date cur = df.parse(arr[i]);
				arrLst.add(cur);
				sum += (float) cur.getTime();
			}
			} catch (Exception e) {
			// 
		}
		long count = arrLst.size();		
		ret += DLMT + String.valueOf(count);

		float mean = sum / count;
		ret += DLMT + String.valueOf(new Date((long)mean));

		Date[] MedianMinMax = calcMedianMinMax(arrLst, mean, count);
		ret += DLMT + String.valueOf(MedianMinMax[0]); // median
		ret += DLMT + String.valueOf(MedianMinMax[1]); // min
		ret += DLMT + String.valueOf(MedianMinMax[2]); // max

		float StdDev = calcStdDev(arrLst, mean);
		ret += DLMT + String.valueOf(new Date((long) StdDev)); 

		Map<Integer, String> Bins =  Discretize(arrLst, MedianMinMax[1], MedianMinMax[2]);

		float gini = calcGiniCoefficient(arrLst, mean);
		ret += DLMT + String.valueOf(gini); // Gini Coefficient Index 

		float Entropy = calcEntropy(Bins, count);
		ret += DLMT + String.valueOf(Entropy); // Shannon Entropy

		float Theil = calcTheil(Bins, count, sum);
		ret += DLMT + String.valueOf(Theil); // Theil

		return ret;
	}	
	
	////////////////////////////////////////////////////////////////////////////////////////

	
	
	////////////////////////////////////////////////////////////////////////////////////////	
	/////  Test Functions /////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	/////////////////////////////////////////////////////////////////////////////////////////
	private static List<Float> generateTempData () {
		String inp = populateTestData();
		String[] arr = inp.split(SC.DLMT);
		
		List<Float> arrLst = new ArrayList<Float>();
		int i=0;
		float f = 0;
		int points = arr.length;
	    
    	try {
    		for (i=0; i<points; i++) {
				Date cur = df.parse(arr[i]);
			    f = (float) cur.getTime();
			    arrLst.add(f);
				}
			} catch (Exception e) {
			//
	    // System.out.println(f);	
	    }
	    Collections.sort(arrLst);    
	    return arrLst;
	}
	
	/////////////////////////////////////////////////////////////////////
	private static void simulate(int count) {
		while (count > 0) {
		List<Float> arrLst = generateTempData();
		Map<Integer, String> Bins = Discretize(arrLst, arrLst.get(0), arrLst.get(arrLst.size()-1));
		
		int i=0, sum = 0; 
		for (i=0; i<binCount; i++) {
			if (Bins.containsKey(i)) {
				String[] arr = (Bins.get(i).toString()).split(SC.DLMT);
				sum += (arr.length-1);
				System.out.print(arr.length-1 + SC.TB);
			}
		}
		System.out.println("Sum: " + sum);
	//	System.out.println(Bins.get(0).toString());
			
		simulate(--count);
		}
	}

	///////////////////////////////////////////////////////////////////////
	private static String populateTestData() {
		String inp = "";
		inp =  "07-07-13,09-12-13,07-07-13,01-06-13,08-06-13,";
		inp += "01-06-13,01-08-08,18-08-10,20-11-12,01-08-11,";
		inp += "01-04-08,01-12-13,01-11-01,10-09-03,10-09-13,";
		inp += "05-08-13,09-05-05,30-07-09,01-07-08,01-04-07,";
		inp += "01-04-08,01-01-08,01-05-11,13-09-99,12-02-04,";
		inp += "12-02-16,12-02-14,12-02-14,16-01-13,12-02-14,";
		inp += "11-08-13,11-08-13,11-08-13,11-08-13,11-08-13,";
		inp += "11-08-13,11-08-13,11-08-13,13-02-06,02-11-13,02-11-13,";
		inp += "02-11-13,02-11-13,02-11-13,02-11-13,02-11-13,02-11-13,12-02-14,";
		inp += "01-08-11,01-08-11,01-03-05,20-10-97,08-09-03,08-09-03,08-09-03,";
		inp += "09-10-02,05-12-13,05-12-13,05-12-13,05-12-13,11-08-13,21-07-13,";
		inp += "21-07-13,21-07-13,21-07-13,21-07-13,21-07-13,21-07-13,21-07-13,";
		inp += "11-10-01,09-05-05,30-11-09,20-10-97,31-05-12,21-03-01,30-05-91,";
		inp += "01-01-08,01-01-08,01-01-08,01-01-08,01-01-08,12-11-04,16-01-13,";
		inp += "29-01-04,05-12-11,05-12-11,01-08-11,25-11-08,13-09-99,22-10-03,";
		inp += "15-04-08,01-04-08,29-03-96,05-12-07,03-08-88,01-08-11,01-11-95,";
		inp += "10-09-13,05-08-13,09-05-05,30-07-09";
		
		return inp;
	}
	/////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////
	
}
