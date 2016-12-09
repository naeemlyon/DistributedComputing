package aggregation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import miscUtils.CommonUtil;
import miscUtils.SC;

public class NumMeasure {
	private String DLMT = "";	
	private static int binCount = 4; // by default value	
	private static double logBase = 2.0;	
	static double infP = Double.POSITIVE_INFINITY;
	static double infN = Double.NEGATIVE_INFINITY;
	
	////////////////////////////////////////////////////////////////////////////////////////
	public void setDLMT(String val) {
		this.DLMT = val;
	}

	public String getDLMT() {
		return DLMT;
	}
	
	
	
	////////////////////////////////////////////////////////////////////////////////////////
	public static void main(String[] args) throws Exception {	
		ThielCorrectExample();
		
	}
	////////////////////////////////////////////////////////////////////////////
	private static Map<Integer, String> Discretize(List<Float> arrLst, float min, float max) {
		Map<Integer, String> Bins = new HashMap<Integer, String>();
		float f =0;
		String s = "";
		int k=0;
		float offSet = (max - min )/ binCount;
//		System.out.println("....offSet:" + offSet);
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
	private static float calcGiniCoefficient(ArrayList<Float> arrVal, float mean) {
		if (arrVal.size() < 1) return 0;  // c'est introuble
		if (arrVal.size() == 1) return 0; 
		if (mean == 0.0) return 0; // means every observation is zero
		
		float ret = 0;
		for (int i = 0; i < arrVal.size(); i++)	{
			for (int j = 0; j < arrVal.size(); j++)	{
				if (i == j) continue;
				ret += (Math.abs(arrVal.get(i) - arrVal.get(j)));
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
	
//		System.out.println("totalObs:logBase: " + totalObs + ":" + logBase
//				+ " (Suggested)bincount: " + binCount);
//		System.out.print("freq:entr:lg -> ");

		for (int i=0; i<binCount; i++) {
			if (Bins.containsKey(i)) {
				String[] arr = (Bins.get(i).toString()).split(SC.DLMT);
				int freq= arr.length-1;
				
				entropy = (float) freq/(float) totalObs;
				lg = Math.log(Double.parseDouble(String.valueOf(entropy)))
							/Math.log(logBase);
		//		System.out.print(freq +":" + SC.Formate(entropy) + ":" + SC.Formate(lg) + SC.TB);
				if (Double.isInfinite(lg) == false ) {
					ret +=  entropy * (float) lg;			
				}
			}
		}
		return -1 * ret; // // Entropy 
	}
	
	///////////////////////////////////////////////////////////////////////////////////

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
	
	///////////////////////////////////////////////////////////////////////////
	private static float localSum (String[] arr) {
		float sum=0f;
		for (int i=0; i<arr.length; i++){
			if (!CommonUtil.isNullOrBlank(arr[i])) {
				sum += Float.parseFloat(arr[i]);
			}
		}
		return sum;
	}
	
	///////////////////////////////////////////////////////////////////////////
	// calculate standard deviation
	private static float calcStdDev(ArrayList<Float> arrVal, float mean) {
		float sumOfSquares = 0.0f;
		float ret = 0; // default value if only one observation
		long count = arrVal.size();
		for (Float v : arrVal) {
			sumOfSquares += (v - mean) * (v - mean);	
		}
		
		if (count > 1) {
			ret = (float) Math.sqrt(sumOfSquares / (count - 1));	
		}
		return ret; // Standard Devision 
	}
	////////////////////////////////////////////////////////////////////////////////////////
	private static float[] calcMedianMinMax(ArrayList<Float> arrVal, float mean, long count) {
		// 0-Median, 1-Min, 2-Max
		float[] ret = new float[3];
		try {
			// sort commentLengths to calculate median, min, max
			Collections.sort(arrVal); // asceding order by default
			// if commentLengths is an even value, average middle two elements
			if (count % 2 == 0) {
				ret[0] = ((arrVal.get((int) count / 2 - 1) + arrVal
						.get((int) count / 2)) / 2.0f);
			} else {
				// else, set median to middle value
				ret[0] = arrVal.get((int) count / 2);
			}
			
			ret[1] = arrVal.get(0);                // First element
			ret[2] = arrVal.get(arrVal.size()-1); // Last element		    
		}
		catch (IndexOutOfBoundsException e) {
		//	System.out.println(e.getMessage());
		//	System.out.println("** arrVal.size(): " + arrVal.size() + " **");			
		}			
		return ret;
	}
	////////////////////////////////////////////////////////////////////////////////////////
	
	private static double[] doRegression (ArrayList<Float> input) {
		double[] ret = new double[2];
		int sz = input.size();
		
		// vector with less than 3 observations
		if (sz <=1) {
			ret[0] = 1.0;
			ret[1] = 1.0;
			return ret;
		}
				
		double[] x = new double[sz-1]; 
		double[] y = new double[sz-1];
		int i=0;
		
    	try {
        	x[0] = (float) input.get(i);
        	for (i=1; i< (sz-1); i++) {
        		x[i] = (float) input.get(i);
        		y[i-1] = (float) input.get(i);    		
        	}
        	y[i-1] = (float) input.get(i);    		
    	}
    	catch (Exception e) {
    		//
    	}    	
    	
    	ret = Regression.CurveFitting(x, y, false);
    	return ret;
	}
	
	////////////////////////////////////////////////////////////////////////////////////////
	
	public String Process (String inp) {
		String ret = SC.ZeroSpace;
		float sum = 0;
		
		String[] arr = inp.split(SC.DLMT);
		int i=0;
		ArrayList<Float> arrLst = new ArrayList<Float>();
		
		for (i=1; i<arr.length; i++) {
			try {
				float f = Float.parseFloat(arr[i]);
				arrLst.add(f);
				sum += f;
			} catch (Exception e) {
				// 
			}
		}
		
		long count = arrLst.size();		
		ret += DLMT + String.valueOf(count);
		
		float mean = sum / count;		
		ret += DLMT + String.valueOf(mean);
		
		// System.out.println("sum=" + sum + "  count=" + count + " mean=" + mean);
		
		float[] MedianMinMax = calcMedianMinMax(arrLst, mean, count);

		ret += DLMT + String.valueOf(MedianMinMax[0]); // median
		ret += DLMT + String.valueOf(MedianMinMax[1]); // min
		ret += DLMT + String.valueOf(MedianMinMax[2]); // max
	
		float StdDev = calcStdDev(arrLst, mean);
		ret += DLMT + String.valueOf(StdDev); 
	
		Map<Integer, String> Bins =  Discretize(arrLst, MedianMinMax[1], MedianMinMax[2]);
		
		float gini = NumMeasure.calcGiniCoefficient(arrLst, mean);
		ret += DLMT + String.valueOf(gini); // Gini Coefficient Index 
		
		float Entropy = calcEntropy(Bins, count);
		ret += DLMT + String.valueOf(Entropy); // Shannon Entropy
		
		float Theil = calcTheil(Bins, count, sum);
		ret += DLMT + String.valueOf(Theil); // Theil
		
		double[] R2 = doRegression(arrLst);
		ret += DLMT + String.valueOf(R2[0]); // R2
		ret += DLMT + String.valueOf(R2[1]); // Degree
	
		return ret;
	}	
	
	////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////

	
	/////  Test Functions /////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	@SuppressWarnings("unused")
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
		
		simulate(--count);
		}
	}

	/////////////////////////////////////////////////////////////////////////////////////////
	private static List<Float> generateTempData () {
		List<Float> arrLst = new ArrayList<Float>();
		int i=0;
		float f = 0;
		int points = 20000;
	    for (i=0; i<points; i++) {
	     f = (float) ( Math.round(Math.random() * binCount ));
	     arrLst.add(f);
	    // System.out.println(f);	
	    }
	    Collections.sort(arrLst);    
	    return arrLst;
	}
	
	///////////////////////////////////////////////////////////////////////////
	private static void ThielCorrectExample () {
		double p1=2,p2=4,p3=6,p4=4,p5=2;
		double P = p1+p2+p3+p4+p5;
		double gs1 = 10, gs2 = 8, gs3 = 6, gs4 = 4, gs5 = 2;
		double totalGsalary = gs1 + gs2 + gs3 + gs4 + gs5;
		double mean = totalGsalary / 5.0; // totalSum/populationGroupCount 
		
		double t = (p1/P)*(gs1/mean)*Math.log(gs1/mean)+
				(p2/P)*(gs2/mean)*Math.log(gs2/mean) +
				(p3/P)*(gs3/mean)*Math.log(gs3/mean) +
				(p4/P)*(gs4/mean)*Math.log(gs4/mean) +
				(p5/P)*(gs5/mean)*Math.log(gs5/mean);
		System.out.println("theil: " + t);
	}

	///// End of  Test Functions ////////////////////////////////////////////

	
	
}
