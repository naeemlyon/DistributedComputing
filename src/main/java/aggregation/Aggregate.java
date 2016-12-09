package aggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import miscUtils.CommonUtil;

public class Aggregate  {

	static CommonUtil cmn = new CommonUtil();
	//////////////////////////////////////////////////////////////////////////////////////////////////
	public static class AverageMapper extends Mapper<Object, Text, Object, Text> {
		
		private Text outKey = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException,
				InterruptedException {
			String DLMT = context.getConfiguration().get("DLMT");
			String Keys = context.getConfiguration().get("Key");
			String Val = "";

		   	if (!CommonUtil.isNullOrBlank(value.toString())) {
				
				if (!CommonUtil.isNullOrBlank(context.getConfiguration().get("ValNum"))) {
					Val += DLMT + context.getConfiguration().get("ValNum");	
				}
	
				if (!CommonUtil.isNullOrBlank(context.getConfiguration().get("ValDate"))) {
					Val += DLMT + context.getConfiguration().get("ValDate");	
				}
				
				if (!CommonUtil.isNullOrBlank(context.getConfiguration().get("ValText"))) {
					Val += DLMT + context.getConfiguration().get("ValText");	
				}
				
			    if (!CommonUtil.isNullOrBlank(context.getConfiguration().get("ValOther"))) {
					Val += DLMT + context.getConfiguration().get("ValOther");	
				}
			    
		   	Val = Val.substring(1);
		   	
		 	Map<String, String> parsed = CommonUtil.SeparateCommonIDs(value.toString(), Keys, Val, DLMT);		
			outKey.set(parsed.get("k"));
			String data = parsed.get("v");
		//	System.out.println(data);
			context.write(outKey, new Text (data));		
		   	}
		}
	}
	
	
	//////////////////////////////////////////////////////////////////////////////////////////////////
	
	public static class AverageReducer extends
			Reducer<Object, Text, Object, Text> {
		
		private static String DLMT = "";
		private static NumMeasure NM = new NumMeasure();
		private static DateMeasure DM = new DateMeasure();
		private static TextMeasure TX = new TextMeasure();

		private static String[] arrNum = null;
		private static String[] arrDate = null;
		private static String[] arrText = null;
		private static String[] arrOther = null;

		private static int lenNum = 0;
		private static int lenDate = 0;
		private static int lenText = 0;
		private static int lenOther = 0;

		private static int totalVal = 0;		
		
		////////////////////////////////////////////////
		public void setup(Context context) {
			// Get the type of join from our configuration
			//joinType = context.getConfiguration().get("joinType");		
			DLMT = context.getConfiguration().get("DLMT");
			
			if (!CommonUtil.isNullOrBlank(context.getConfiguration().get("ValNum"))) {
			 arrNum = context.getConfiguration().get("ValNum").split(DLMT);
			 lenNum = arrNum.length;
			}
			
			if (!CommonUtil.isNullOrBlank(context.getConfiguration().get("ValDate"))) {
			 arrDate = context.getConfiguration().get("ValDate").split(DLMT);
 			 lenDate = arrDate.length;	
			}
			
			if (!CommonUtil.isNullOrBlank(context.getConfiguration().get("ValText"))) {
			 arrText = context.getConfiguration().get("ValText").split(DLMT);
			 lenText = arrText.length;	
			}
			
			if (!CommonUtil.isNullOrBlank(context.getConfiguration().get("ValOther"))) {
			 arrOther = context.getConfiguration().get("ValOther").split(DLMT);
			 lenOther = arrOther.length;	
			}
			
			totalVal = lenNum + lenDate + lenText + lenOther;
		}
		
		
		//////////////////////////////////////////////////
		public void reduce(Object key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String[] arr = null;
			String tmp = "";
			ArrayList<String> V = new ArrayList<String>();
			V.clear();
			int i=0;
			for (i=0; i<totalVal; i++) {
				V.add("");
			}

			// Iterate through all input values for this key
			for (Text val : values) {
				arr =  val.toString().split(DLMT);
				for (i=0; i<arr.length; i++) {
					if (arr[i].equalsIgnoreCase("NA")==false) {
						tmp = V.get(i) + DLMT + arr[i];
						V.set(i, tmp);	
					}
				}					
			}
			
			String ret = "";
			NM.setDLMT(DLMT);
			DM.setDLMT(DLMT);
			TX.setDLMT(DLMT);
			
			
			i=1;
			
	//		if (key.toString().equalsIgnoreCase("J") == true )
	//		    ret += NM.Process(V.get(i));
			
			
			int offset=0;
			for (i=0; i<lenNum; i++) {
				ret += NM.Process(V.get(offset + i));
			}
			
			offset += lenNum;
			for (i=0; i<lenDate; i++) {
				ret += DM.Process(V.get(offset + i));
			}
			
			offset +=  (lenDate);
			for (i=0; i<lenText; i++) {
				ret += TX.Process(V.get(offset + i));
			}
		
			offset += lenText;
			for (i=0; i<lenOther; i++) {
//				ret += TX.Process(V.get(offset + i));
			}
			
			context.write(key, new Text (ret.toString()));
		}
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////////////

	public static void main(String[] args) throws Exception {	
		Configuration conf = new Configuration();		
		Job job = new Job(conf, "Summarization (Average)");
		job.setJarByClass(Aggregate.class);
		job.setMapperClass(AverageMapper.class);
		// job.setCombinerClass(AverageReducer.class);
		job.setReducerClass(AverageReducer.class);
		job.setOutputKeyClass(Text.class); // i have changed this one
		job.setOutputValueClass(Text.class);
		
		String input = cmn.PrepareConfigurationData(args, "input");
		String output = cmn.PrepareConfigurationData(args, "output");		
		job.getConfiguration().set("output", output);
				
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
				
		String Key = cmn.PrepareConfigurationData(args, "Key");		
		job.getConfiguration().set("Key", Key);
		
		//String Val = CommonUtil.PrepareConfigurationData(args, "Val");
		//job.getConfiguration().set("Val", Val);
		
		String ValNum = cmn.PrepareConfigurationData(args, "ValNum");
		job.getConfiguration().set("ValNum", ValNum);
		
		String ValDate = cmn.PrepareConfigurationData(args, "ValDate");
		job.getConfiguration().set("ValDate", ValDate);
		
		String ValText = cmn.PrepareConfigurationData(args, "ValText");
		job.getConfiguration().set("ValText", ValText);
		
		String ValOther = cmn.PrepareConfigurationData(args, "ValOther");
		job.getConfiguration().set("ValOther", ValOther);
		
		String DLMT = cmn.PrepareConfigurationData(args, "DLMT");
		job.getConfiguration().set("DLMT", DLMT);
		
		// delete the outout folder at hdfs if it exists already
		Path OutputPath = new Path(output); // output path	
		FileOutputFormat.setOutputPath(job, OutputPath);		
		cmn.deleteFolder(conf, OutputPath);		
			
		System.exit(job.waitForCompletion(true) ? 0 : 3);
		
	}
	

	/////////////////////////////////////////////////////////////////////////////////////////
	
	
}
