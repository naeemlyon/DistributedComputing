package aggregation;

import java.math.BigDecimal;
import java.util.Arrays;
import org.rosuda.REngine.Rserve.*;
import miscUtils.Config;
import org.apache.commons.math3.fitting.*;

/////////////////////////////////////////////////////////////////////////////
public class Regression {
	
	static Config Conf = new Config();
	private static String regScale = (String) Conf.prop.getProperty("regScale");
    
	/////////////////////////////////////////////////////////////////////
	public static void main(String[] args) throws RserveException {
    	//double[] x = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 4, 8}; 
		double[] x = { 2, 3, 6, 11, 18, 27, 38, 51, 66, 83, 102, 321, 654, 3, 8};
		double[] y = { 3, 6, 11, 18, 27, 38, 51, 66, 83, 102, 321, 654, 3, 8, 7};
    	
    	
    	boolean Repeatition = false;	
    	double[] R2 = CurveFitting(x, y, Repeatition);
    	System.out.println("Best R2 : " + Arrays.toString(R2));
    	
	}
    
    
    
    /////////////////////////////////////////////////////////////////////
    public static double[] CurveFitting(double[] x, double[] y, boolean repeatition) {
    	int degree = 0;
    	int degreeOptimized = 0;
    	int maxDegree = 50;
    	double current = 0.0;
    	double prev = 0.0;
    	double R2 = Double.MIN_VALUE;
     
    	while ((++degree) <= maxDegree) {
    		current = CurveFitting(degree, x, y);
    		current = RoundUp(current, -1);
    		
    	    double diff = current - R2;
    	    
    	    // improvement observed
    		if ( diff > 0) {
    			R2 =  current;
    			degreeOptimized = degree;    			
    		} 
    		
    		// System.out.println(degree + " : " + current);
    		
    		// regression reaches its ultimate level
			// no difference by increasing degree    		
    		if (repeatition == false) {
    			diff = current - prev;
    			
    			//System.out.println( "cur:" + current + "\tprev:" + prev + "\tdiff= " + diff);
        		
        		if (diff == 0.0)  {
        			degree = maxDegree + 1;        			
        		}        			        		
        		prev = current;	
    		}
    	}
    	
    	double[] ret = new double[2];
    	ret[0] = R2;
    	ret[1] = degreeOptimized;
    	return ret;
    }
    
    ///////////////////////////////////////////////////////////////////////
    private static double CurveFitting(int degree, double[] x, double[] y) {
        final WeightedObservedPoints obs = new WeightedObservedPoints();
        for (int i=0; i< x.length; i++){
            obs.add(x[i], y[i]);
        }
        
        PolynomialCurveFitter fitter = PolynomialCurveFitter.create(degree);
                
        double R2 = 0.0;
        
        try {
        	double[] coeff = fitter.fit(obs.toList());
            R2 = CurveFittingAnalysis(x, y, coeff);            
        } catch (Exception e) {
        		
        }
        
        return R2;
    }
    
    /////////////////////////////////////////////////////////////////////
    private static double CurveFittingAnalysis(double[] x, double[] y, double[] coeff) {
    	double R2 = 0.0;
    	double SSe = 0.0;
    	double SSt = 0.0;
    	double diff = 0.0;
    	double sum = 0.0;
    	double Ymean = 0.0;
    	int len = x.length;
    	int i=0, j=0;
    	
    	double[] ym = new double[len];
    	
    	for (i=0; i<len; i++) {
    		for (j=0; j<coeff.length; j++) {
    			ym[i] += (coeff[j] * Math.pow(x[i], j));
    		}
    		sum +=y[i];	//System.out.print(ym[i] + "\t");
    	}
    	
    	Ymean = sum / len;
    	
    	for (i=0; i<len; i++) {
    		diff = y[i] - ym[i];
    		diff *= diff;
    		SSe += diff;
    		
    		diff = y[i] - Ymean;
    		diff *= diff;
    		SSt += diff;	
    	}
    	
    	R2 = SSe/SSt;
    	return (1-R2);
    }
    
    
    

    /////////////////////////////////////////////////////////////////////
    private static double RoundUp(double input, int scale) {
    	// retrieve from config.properties
    	if (scale == -1)  
    		scale = Integer.parseInt(regScale);
    	
    	return (new BigDecimal(input).setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue());
    }
    
   
    /////////////////////////////////////////////////////////////////////

    
}



