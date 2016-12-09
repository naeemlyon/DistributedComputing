package miscUtils;

import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REngineException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;


/*
 **** Steps in RStudio ************
  install.packages("Rserve")
  library(Rserve)
  Rserve()
    
 ****** Maven Dependency *********
   <dependency>
	<groupId>org.rosuda.REngine</groupId>
	<artifactId>Rserve</artifactId>
	<version>1.8.1</version>
	</dependency>
**********************************	
 */


public class ConnectRStudio {
	
	static RConnection R = null;
	
	
	
	/////////////////////////////////////////////////////////////////////
	 public static void main(String[] args) throws RserveException {
		 Connect();
		 Exploit ();
	 }
	/////////////////////////////////////////////////////////////////////
    public ConnectRStudio () {
    	Connect();
    }
    ////////////////////////////////////////////////////////////////////
    private static void Connect() {
    	
        	try {
        		// make a new local connection on default port (6311)
                R = new RConnection();        
            } catch (REngineException e) {
            	
            }
    }
    ////////////////////////////////////////////////////////////////////
    private static void Exploit () {
    	double d[];
    	org.rosuda.REngine.REXP x0 = null;
		try {
			d = R.eval("rnorm(100)").asDoubles();
			x0 = R.eval("R.version.string");
			System.out.println(x0.asString());
		    System.out.println(d.length + "");
		} catch (RserveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
       
    }
    

}
