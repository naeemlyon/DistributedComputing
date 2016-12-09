package reducejoin;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import miscUtils.CommonUtil;

public class JoinMapperA extends Mapper<LongWritable, Text, Text, Text>{

	private Text outkey = new Text();
	private Text outvalue = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String DLMT = context.getConfiguration().get("DLMT");
		String KeysA = context.getConfiguration().get("KeysA");
		String ValA = context.getConfiguration().get("ValA");
		
		Map<String, String> parsed = CommonUtil.SeparateCommonIDs(value.toString(), KeysA, ValA, DLMT);		
		outkey.set(parsed.get("k"));
			
		// Flag this record for the reducer and then output
		outvalue.set("A" + parsed.get("v"));
		context.write(outkey, outvalue);
	}
}
