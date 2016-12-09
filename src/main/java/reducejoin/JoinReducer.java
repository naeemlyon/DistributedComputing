package reducejoin;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {

	private ArrayList<Text> listA = new ArrayList<Text>();
	private ArrayList<Text> listB = new ArrayList<Text>();
	private static String dummyA = new String();
	private static String dummyB = new String();
	private static boolean isValA_Null = false;	
	
	private String joinType = null;	
	private static String NA = "NA";
	private static String DLMT = "";
	
	
	/////////////////////////////////////////////////////////////////////	
	@Override
	public void setup(Context context) {
		// Get the type of join from our configuration
		joinType = context.getConfiguration().get("joinType");		
		DLMT = context.getConfiguration().get("DLMT");		
		SetDummyString(context);
	}
	
	/////////////////////////////////////////////////////////////////////////
	private static void SetDummyString(Context context) {
		
		dummyA = context.getConfiguration().get("ValA");		
		String[] arr = dummyA.split(DLMT);
		isValA_Null = dummyA.contains("null");
		dummyA = "";		
		for (@SuppressWarnings("unused") String tmp: arr) {
		 dummyA += NA + DLMT ;
		}
		////// notice the DLMT position 
		// dummyB = context.getConfiguration().get("ValB");
		dummyB = context.getConfiguration().get("ValB");;
		arr = dummyB.split(DLMT);
		dummyB = "";
		for (@SuppressWarnings("unused") String tmp: arr) {
		 dummyB += DLMT +  NA;
		}		
	}
	
    ////////////////////////////////////////////////////////////////////////////////
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

	// Clear our lists
		listA.clear();
		listB.clear();
	//	System.out.println( key.toString());
		
		// iterate through all our values, binning each record based on what
		// it was tagged with
		// in the end, remove the tag!
		
		for (Text t : values) {
	//		  System.out.print(t.toString() + "\t");
			if (t.charAt(0) == 'A') {
				listA.add(new Text(t.toString().substring(1)));
			} else if (t.charAt(0) == 'B') {
				listB.add(new Text(t.toString().substring(1)));
			}
		}
		
	//	System.out.print(listA.size() + " :::::: " + listB.size() + "\n");
	// Execute our join logic now that the lists are filled
		
		executeJoinLogic(new Text( key + DLMT), context);
	}

	private void executeJoinLogic(Text ky, Context context) throws IOException,
	InterruptedException {
		if (joinType.equalsIgnoreCase("inner")) {
			// If both lists are not empty, join A with B
			if (!listA.isEmpty() && !listB.isEmpty()) {
				for (Text A : listA) {
					for (Text B : listB) {
						// if valA/B is null, all of the values from A/B are in keyset
							context.write(ky, new Text (A + DLMT + B));
					}
				}
			}
		} else if (joinType.equalsIgnoreCase("leftouter")) {
			// For each entry in A,
			for (Text A : listA) {
				// If list B is not empty, join A and B
				if (!listB.isEmpty()) {
					for (Text B : listB) {
						// if valA/B is null, all of the values from A/B are in keyset
						if (isValA_Null) {
							context.write(ky, new Text (B.toString().substring(1)));
						}else {
							context.write(ky, new Text (A + DLMT  + B));	
						}
							
							
					}
				} else {
					// Else, output A by itself		
					// System.out.println("L.out = " + ky.getLength() + " .. " +  A.getLength());
					if (isValA_Null) {
						context.write(ky, new Text (dummyB.substring(1) ));
					} else {
						context.write(ky, new Text (A + dummyB));
					}	
				}
			}
			
		} else if (joinType.equalsIgnoreCase("rightouter")) {
			// FOr each entry in B,
			for (Text B : listB) {
				// If list A is not empty, join A and B
				if (!listA.isEmpty()) {
					for (Text A : listA) {						
						// if valA/B is null, all of the values from A/B are in keyset
							context.write(ky, new Text (A + DLMT + B));	
					}
				} else {
					// Else, output B by itself			
					// System.out.println("R.out = " + ky);
					if (isValA_Null) {
						context.write(ky, new Text (B));
					} else {
						context.write(ky, new Text (dummyA + B));
					}						
				}
			}
		} else if (joinType.equalsIgnoreCase("fullouter")) {
			// If list A is not empty
			if (!listA.isEmpty()) {
				// For each entry in A
				for (Text A : listA) {
					// If list B is not empty, join A with B
					if (!listB.isEmpty()) {
						for (Text B : listB) {
							// if valA/B is null, all of the values from A/B are in keyset							
							context.write(ky, new Text (A + DLMT + B));	
						}
					} else {
						// Else, output A by itself
						if (A.getLength() == 0) {
							context.write(ky, new Text(dummyB.substring(1)));
						} else {
							context.write(ky, new Text(A + dummyB));	
						}
					}
				}
			} else {
				// If list A is empty, just output B
				for (Text B : listB) {
					if (isValA_Null) {
						context.write(ky, new Text (B));
					} else {
						context.write(ky, new Text (dummyA + B));
					}	
				}
			}
		} else if (joinType.equalsIgnoreCase("anti")) {
			// If list A is empty and B is empty or vice versa
			if (listA.isEmpty() ^ listB.isEmpty()) {

				// Iterate both A and B with null values
				// The previous XOR check will make sure exactly one of
				// these lists is empty and therefore won't have output
				for (Text A : listA) {
					if (isValA_Null) {
						context.write(ky, new Text (dummyB.substring(1)));
					} else {						
						context.write(ky, new Text (A + dummyB));
					}	
				}

				for (Text B : listB) {
					if (isValA_Null) {
						context.write(ky, new Text (B));
					} else {											
						context.write(ky, new Text(dummyA + B));
					}	
				}
			}
		} else {
			throw new RuntimeException(
					"Join type not set to inner, leftouter, rightouter, fullouter, or anti");
		}
	}
}