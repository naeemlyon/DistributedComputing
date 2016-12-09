package miscUtils;

import jdistlib.disttest.DistributionTest;

import jdistlib.*;

public class dist {
	private static DistributionTest dt = new DistributionTest();
	private static int sz = 500;
	private static double Mean=43.0;
	private static MathUtil MU = new MathUtil();

	///////////////////////////////////////////////////////////////////////////

    public static void main( String[] args )
    {
     	cauchy();
    	chidist();
    	testDist();
    }
    

    
	///////////////////////////////////////////////////////////////////////////// 
	private static void chidist() {
//		Chi c = new Chi(3.0);
		//ChiSquare c = new ChiSquare(3.0);
		Normal c = new Normal();
		c.mu = Mean;
		
		double x = 25.24;
		//    double location = 0.9, scale = 0.9;
		
		double[] r = new double[4];
	//	System.out.println ("random" + "\t" + "dens" + "\t" + "cumu" + "\t" +	"hazard" + "\t");
		   
		double sum =0.0;	
		for (int i=0; i<sz; i++) {
			r[0] = c.random();
			sum += r[0];
//			x = r[0];
			r[1] = c.density(x, true);
			r[2] = c.cumulative(x);
			r[3] = c.hazard(x, true);
		/*	
			System.out.println (
				MU.RoundUp(r[0]) + "\t" + 
				MU.RoundUp(r[1]) + "\t" +
				MU.RoundUp(r[2]) + "\t" +
				MU.RoundUp(r[3]) + "\t");
		*/
		}
		System.out.println ("Normal Mean: " + MU.RoundUp(sum / sz,2) );
	}


    
   ///////////////////////////////////////////////////////////////////////////// 
    private static void cauchy() {
    	Cauchy c = new Cauchy(Mean, 0.2);
		double sum =0.0;	
    	
    	double x = 25.24;
    //    double location = 0.9, scale = 0.9;
        
		double[] r = new double[4];
	//	System.out.println ("random" + "\t" + "dens" + "\t" + "cumu" + "\t" +	"hazard" + "\t");
		   	
		for (int i=0; i<sz; i++) {
			r[0] = c.random();	
			sum += r[0];
			x = r[0];
			r[1] = c.density(x, true);
			r[2] = c.cumulative(x);
			r[3] = c.hazard(x, true);
		/*	
			System.out.println (
					MU.RoundUp(r[0]) + "\t" + 
					MU.RoundUp(r[1]) + "\t" +
					MU.RoundUp(r[2]) + "\t" +
					MU.RoundUp(r[3]) + "\t");
		*/ 
		}
		System.out.println ("Cauchy Mean: " + sum / sz );
    }
	
    /////////////////////////////////////////////////////////////////////////////
	@SuppressWarnings("static-access")
	public static void testDist() {
		
		double[] x = {2.0,3.2,3.5,3.9,6.9,9.6,6.5,3.7};
		double[] y = {1.2,2.3,5.5,5.9,1.9,6.6,1.5,2.7};
		boolean force_exact = true;
		
	    double[] ret = dt.ansari_bradley_test(x, y, force_exact);	 
	    System.out.println(SC.NL + "ansari_bradley: " + ret[0] + SC.TB + "p-val: " + ret[1]);
	    
	    ret = dt.kolmogorov_smirnov_test(x, y, force_exact);
	    System.out.println(SC.NL + "kolmogorov_smirnov: " + ret[0] + SC.TB + "p-val:" + ret[1]);
	    
	}
	////////////////////////////////////////////////////////////////////////////
}

//////////////////////////////////////////
