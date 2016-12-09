package reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import miscUtils.CommonUtil;
import miscUtils.SC;

public class JoinDriver {

	static CommonUtil cmn = new CommonUtil();
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();		 
		Job job = new Job(conf, "Reduce Side Join");
		/*
		conf.set("dfs.socket.timeout", "90000");
		conf.set("dfs.client.socket-timeout", "90000");
		conf.set("dfs.datanode.socket.write.timeout", "90000");
		*/
		
		String output = cmn.PrepareConfigurationData(args, "output");		
		job.getConfiguration().set("output", output);
		
		// Use multiple inputs to set which input uses what mapper
		// This will keep parsing of each data set separate from a logical
		// standpoint
		// However, this version of Hadoop has not upgraded MultipleInputs
		// to the mapreduce package, so we have to use the deprecated API.
		// Future releases have this in the "mapreduce" package.
		String input = cmn.PrepareConfigurationData(args, "input");
		String[] inp = input.split(SC.SPACE);		

		MultipleInputs.addInputPath(job, new Path(inp[0].trim()), TextInputFormat.class, JoinMapperA.class);
		
		MultipleInputs.addInputPath(job, new Path(inp[1].trim()), TextInputFormat.class, JoinMapperB.class);
		
		// Configure the join type
		String joinType = cmn.PrepareConfigurationData(args, "joinType");		
		if (!(joinType.equalsIgnoreCase("inner")
				|| joinType.equalsIgnoreCase("leftouter")
				|| joinType.equalsIgnoreCase("rightouter")
				|| joinType.equalsIgnoreCase("fullouter") || joinType
					.equalsIgnoreCase("anti"))) {
			System.err
					.println("Join type not set to inner, leftouter, rightouter, fullouter, or anti");
			System.exit(2);
		}	
		job.getConfiguration().set("joinType", joinType.toLowerCase());		
		
		String KeysA = cmn.PrepareConfigurationData(args, "KeysA");		
		job.getConfiguration().set("KeysA", KeysA);
		
		String ValA = cmn.PrepareConfigurationData(args, "ValA");
		job.getConfiguration().set("ValA", ValA);
		
		String DLMT = cmn.PrepareConfigurationData(args, "DLMT");
		job.getConfiguration().set("DLMT", DLMT);
				
		String KeysB = cmn.PrepareConfigurationData(args, "KeysB");		
		job.getConfiguration().set("KeysB", KeysB);
		
		String ValB = cmn.PrepareConfigurationData(args, "ValB");
		job.getConfiguration().set("ValB", ValB);
				
		job.setJarByClass(JoinDriver.class);


		job.setReducerClass(JoinReducer.class);

		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// delete the outout folder at hdfs if it exists already
		Path OutputPath = new Path(output); // output path	
		FileOutputFormat.setOutputPath(job, OutputPath);		
		cmn.deleteFolder(conf, OutputPath);			
		
		System.exit(job.waitForCompletion(true) ? 0 : 3);
	}
	
	
	/////////////////////////////////////////////////////////////////////////////////

	
}
