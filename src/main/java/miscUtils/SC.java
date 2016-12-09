package miscUtils;

import java.text.DecimalFormat;

public class SC {
	public static String DLMT = ","; 
	public static String DOT = ".";
	public static String COLON = ":";
	public static String SPACE = " ";
	public static String QUOTE = "\"";
	public static String NL = "\n";	
	public static String NL2 = "\n\n";	
	public static String NL3 = "\n\n\n";	
	public static String TB = "\t";	
	public static String TB2 = "\t\t";	
	public static String TB3 = "\t\t\t";	
	public static String TB4 = "\t\t\t\t";	
	public static String MiddleBracketS = "[";
	public static String MiddleBracketE = "]";
	public static String BBracketS = "{";
	public static String BBracketE = "}";	
	public static String ZeroSpace = "";
	public static String FSlash = "/";
	public static String sCOLON = ";";
	public static String US = "_";
	public static String PIPE = "|";
	public static String GT = ">";
	public static String ST = "<";
	public static String EQ = "=";
	
		
	public static void main(String[] args) {
		
	}
	
	///////////////////////////////////////////////////////////////////
	public static String Formate (double d) {
		String pattern = "##.##";
		DecimalFormat decimalFormat = new DecimalFormat(pattern);
		String ret = decimalFormat.format(d);
		return ret;
	}
	///////////////////////////////////////////////////////////////////

	public static String Formate (float d) {
		String pattern = "##.##";
		DecimalFormat decimalFormat = new DecimalFormat(pattern);
		String ret = decimalFormat.format((double)d);
		return ret;
	}
	///////////////////////////////////////////////////////////////////


	 
}
