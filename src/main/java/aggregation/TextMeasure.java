package aggregation;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import miscUtils.CommonUtil;
import miscUtils.SC;

public class TextMeasure {
	private String DLMT = "";	
	private static double logBase = 2.0;	

	////////////////////////////////////////////////////////////////////////////////////////
	public void setDLMT(String val) {
		this.DLMT = val;
	}

	public String getDLMT() {
		return DLMT;
	}
	////////////////////////////////////////////////////////////////////////////////////
		
	////////////////////////////////////////////////////////////////////////////////////////
	/*
	 * marginal	entropy of X , 
	 * and its mutual information with itself.	
	 */
	////////////////////////////////////////////////////////////////////////////////////////
	private static float calcEntropy(Map<String, Long> Bins, long totalObs) {
		float ret = 0.0f;
		float entropy = 0.0f;
		double lg = 0.0;
	
//		System.out.println("totalObs:logBase: " + totalObs + ":" + logBase
//				+ " (Suggested)bincount: " + Bins.size());
//		System.out.print("freq:entr:lg -> ");

		 Iterator <Entry <String, Long>> it = Bins.entrySet().iterator();
		 Entry <String, Long> pair = null;
		 
		 while (it.hasNext()) { 
			pair = it.next();
			long freq= pair.getValue();
			
			entropy = (float) freq/(float) totalObs;
			lg = Math.log(Double.parseDouble(String.valueOf(entropy)))
						/Math.log(logBase);
//			System.out.print(freq +":" + SC.Formate(entropy)  + ":" + SC.Formate(lg) + SC.TB);
			if (Double.isInfinite(lg) == false ) {
				ret +=  entropy * (float) lg;			
			}
			
		 }
		return -1 * ret; // // Entropy 
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	
	public String Process (String inp) {
		String ret = SC.ZeroSpace;
		long count = 0L;
		
		String[] arr = inp.split(SC.DLMT);
		int i=0;
		Map<String,Long> Bins = new HashMap<String, Long>();
//		ArrayList<String> input = new ArrayList<String>();
		
		for (i=1; i<arr.length; i++) {
			try {
				if (!CommonUtil.isNullOrBlank(arr[i])) {
					String s = arr[i] ;
					if (Bins.containsKey(s)) {
						Bins.put(s, Bins.get(s) + 1 );
					} else {
						Bins.put(s, 1L);
					}
					count++;
				}
			} catch (Exception e) {
				// 
			}
		}
		
		ret += DLMT + String.valueOf(count);
		
		float Entropy = calcEntropy(Bins, count);
		ret += DLMT + String.valueOf(Entropy); // Shannon Entropy

		
		return ret;
	}	
	
	////////////////////////////////////////////////////////////////////////////////////////
	
	
	
	
	
}
