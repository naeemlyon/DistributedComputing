package bayes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import miscUtils.*;

import org.apache.hadoop.fs.Path;

public class NBTestMapper extends MapReduceBase implements Mapper<LongWritable, Text, NullWritable,Text>{
	String delimiter,continousVariables,discreteVariables,targetClasses;	
	int targetVariable,numColums;
	HashSet<Integer> continousVariablesIndex;
	HashSet<Integer> discreteVariablesIndex;
	Map<String,String> model = new HashMap<String, String>();;
	HashSet<String> classesTargetVariables;
	@SuppressWarnings("unused")
	private final static IntWritable one = new IntWritable(1);
	private static CommonUtil cmn = new CommonUtil();
	private static MathUtil MU = new MathUtil();
		
	@Override
	 public void configure(JobConf conf){
		delimiter = conf.get("delimiter");
		numColums = conf.getInt("numColumns", 0);
		continousVariables = conf.get("continousVariables");
		discreteVariables = conf.get("discreteVariables");
		targetClasses = conf.get("targetClasses");
	    targetVariable = conf.getInt("targetVariable",0);
	    discreteVariablesIndex = new HashSet<Integer>();
	    continousVariablesIndex = new HashSet<Integer>();
	    
	    if(continousVariables!=null)
	    	continousVariablesIndex = cmn.SplitNumericVariables(continousVariables, delimiter);
	    if(discreteVariables!=null)
	    	discreteVariablesIndex = cmn.SplitNumericVariables(discreteVariables, delimiter);
	    classesTargetVariables = cmn.SplitStringVariables(targetClasses, delimiter);
	    
	    try {
			// local mode vs
			Path[] filesIncache = DistributedCache.getLocalCacheFiles(conf);
			for(int i=0;i<filesIncache.length;i++){
				// distributed mode only
				// BufferedReader fis = new BufferedReader(new FileReader(filesIncache[i].getPath().toString()));
				
				@SuppressWarnings("resource")
				BufferedReader fis = new BufferedReader(new FileReader(filesIncache[i].getName().toString()));
				
				String record; 
				 while ((record = fis.readLine()) != null) {
					 String key,value;
					 StringTokenizer tokRecord = new StringTokenizer(record);
					 key = tokRecord.nextToken();
					 value = tokRecord.nextToken();
					 model.put(key, value); 
				 }			
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/////////////////////////////////////////////////////////////////////////////	
	public void map(LongWritable key, Text value,
			OutputCollector<NullWritable, Text> output, Reporter arg3) throws IOException {

		String record = value.toString();
	    int featureID = 1;
	    Double prob=0.0;	    
	    String features[] = record.split(delimiter);
	    
	    
	    String classified = "";
	    double whichOne = Double.MIN_VALUE;
	    
	   // String exam = "";
	    
	    for (String labels : classesTargetVariables){
	    	prob = 1.0;
	    	featureID = 1;
	   // 	exam += "label: " + labels + SC.TB;
	    	
	      for(int i=0; i<numColums; i++){
	    	if(discreteVariablesIndex.contains(featureID)){
	    		
	    	//   output.collect(NullWritable.get(),new Text(featureID + ":" + features[i] + ":" + labels));
	    	   prob *= MU.calculateProbablity(featureID,features[i],labels, model, targetVariable);
	    	}
	    	if(continousVariablesIndex.contains(featureID)){
	    //		output.collect(NullWritable.get(),new Text(featureID + ":" + features[i] + ":" + labels));
	    		prob *= MU.calculateGaussian(featureID,features[i],labels, model);	    	  
	    	}
	    	++featureID;	   	
	     }
	  //    exam += "prob: " + prob + SC.TB;

	   // the model is silent on this class 
    	if (prob.equals(1.0))
    		prob = Double.MIN_VALUE;
	      
	     // decision making phase
	     if ((whichOne < prob) == true) {
	    	whichOne = prob;
	    	classified = labels;
	      }
	 //    exam += "classified: " + classified + SC.NL;
	     
	    } // for (String labels : classesTargetVariables){
	    
	//  String[] Arr = record.split(",");	    
	//	record = Arr[Arr.length-1] + "_" + classified;
		
		record += "_"  + classified;
		
	//	record = exam + SC.NL;
		
		output.collect(NullWritable.get(),new Text(record)); // labelprobablityString
		
	}
}
