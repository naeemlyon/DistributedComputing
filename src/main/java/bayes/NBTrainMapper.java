package bayes;

import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import miscUtils.*;

import org.apache.commons.lang3.math.NumberUtils;

/*
conf.setMapOutputKeyClass(Text.class);
conf.setMapOutputValueClass(DoubleWritable.class);
conf.setOutputKeyClass(Text.class);
conf.setOutputValueClass(Text.class);
*/

public class NBTrainMapper extends MapReduceBase implements Mapper<LongWritable, Text,Text,DoubleWritable>{
   
	String delimiter,continousVariables,discreteVariables;	
	int targetVariable,numColums;
	HashSet<Integer> continousVariablesIndex;
	HashSet<Integer> discreteVariablesIndex;
	private static CommonUtil cmn = new CommonUtil();
	
	///////////////////////////////////////////////////////////////
	@Override
	 public void configure(JobConf conf){
		delimiter = conf.get("delimiter");
		numColums = conf.getInt("numColumns", 0);
		continousVariables = conf.get("continousVariables");
		discreteVariables = conf.get("discreteVariables");
	    targetVariable = conf.getInt("targetVariable",0);
	    discreteVariablesIndex = new HashSet<Integer>();
	    continousVariablesIndex = new HashSet<Integer>();
	    
	    if(continousVariables!=null)
	    	continousVariablesIndex = cmn.SplitNumericVariables(continousVariables, delimiter);
	    if(discreteVariables!=null)
	    	discreteVariablesIndex = cmn.SplitNumericVariables(discreteVariables, delimiter);
	}
	
	/////////////////////////////////////////////////////////////////////
	public void map(LongWritable arg0, Text value,
			OutputCollector<Text, DoubleWritable> output, Reporter arg3)
			throws IOException {
		
		Integer varIndex = 1; 
		String record = value.toString();
		String features[] = record.split(delimiter);
	    for(int i = 0 ;i < numColums ; i++){
	    	if(varIndex!= targetVariable){
	    		
	    		// 2_classState 1.0	    		
	    		if(discreteVariablesIndex.contains(varIndex)) {	    		   
	    			output.collect(new Text(varIndex+"_"+features[i]+"_"+features[targetVariable-1].trim()), new DoubleWritable(1.0));	    		   
	    		}
	    		 
	    		// we can add any marker before or after features[targetVariable-1]
	    		// 1_Iris-setosa 6.12
	    		if(continousVariablesIndex.contains(varIndex))
	    		   if (NumberUtils.isNumber(features[i]) == true) {
	    			  output.collect(new Text(varIndex+"_"+features[targetVariable-1].trim()), new DoubleWritable(Double.parseDouble(features[i])));
	    		   }
	    	}
	    	varIndex ++;
	}	    
	    // 5_Iris-setosa 1.0
	    output.collect(new Text(targetVariable+"_"+features[targetVariable-1].trim()), new DoubleWritable(1.0));
	    
	    // 5 1.0
	    output.collect(new Text(targetVariable+""), new DoubleWritable(1.0));
		
	}
   ///////////////////////////////////////////////////////////////////////////	
}
