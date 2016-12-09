package reducejoin;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import miscUtils.CommonUtil;

public class JoinMapperB extends Mapper<LongWritable, Text, Text, Text>{

	private Text outkey = new Text();
	private Text outvalue = new Text();	
	/////////////////////////////////////////////////////////////////////
		
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String DLMT = context.getConfiguration().get("DLMT");
		String KeysB = context.getConfiguration().get("KeysB");
		String ValB = context.getConfiguration().get("ValB");
		
		// Parse the input string into a nice map
		Map<String, String> parsed = CommonUtil.SeparateCommonIDs(value.toString(), KeysB, ValB, DLMT);
		
		outkey.set(parsed.get("k"));
		
		// Flag this record for the reducer and then output
		outvalue.set("B" + parsed.get("v"));
		context.write(outkey, outvalue);
	}

}
