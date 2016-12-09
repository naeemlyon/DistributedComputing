package bayes;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import miscUtils.*;


/*
conf.setMapOutputKeyClass(Text.class);
conf.setMapOutputValueClass(DoubleWritable.class);
conf.setOutputKeyClass(Text.class);
conf.setOutputValueClass(Text.class);
*/

public class NBTrainReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, NullWritable,Text>{

	String delimiter, continousVariables,discreteVariables;	
	int targetVariable;
	HashSet<Integer> continousVariablesIndex;
	HashSet<Integer> discreteVariablesIndex;
	private static CommonUtil cmn = new CommonUtil();
	
	////////////////////////////////////////////////////////////////	
	@Override
	 public void configure(JobConf conf){
		delimiter = conf.get("delimiter");
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
	
	////////////////////////////////////////////////////////////////
	public void reduce(Text keyId, Iterator<DoubleWritable> values,
			OutputCollector<NullWritable, Text> output, Reporter arg3) throws IOException {

		String id = keyId.toString().split("_")[0];
		if(continousVariablesIndex.contains(Integer.parseInt(id))){
			double sumsqr=0,sum = 0,count=0,tmp;
			double mean,var;
			 while (values.hasNext())
	          {  
			   tmp = values.next().get();
        	   sumsqr+=tmp*tmp;
               sum += tmp;
               count++;
	          }
			 mean=sum/count;
			 var=(sumsqr-((sum*sum)/count))/count;
	     
			// 1_Iris-setosa 5.006,0.121   colNum mean,sd
			 output.collect(NullWritable.get(), new Text(keyId + " " + mean + "," + var));
		}
		if(discreteVariablesIndex.contains(Integer.parseInt(id))){
			Double count = 0.0;
			while (values.hasNext())
	          count +=  values.next().get();
			
			// 2_classState 5642.0  
			output.collect(NullWritable.get(), new Text(keyId + " "+ count.toString()));
		}
		if(targetVariable == Integer.parseInt(id)){
			Double count = 0.0;
			while (values.hasNext())
	          count +=  values.next().get();
			
			
			// 5_Iris-setosa 50.0
			output.collect(NullWritable.get(), new Text(keyId + " " + count.toString()));
		}
	}
	////////////////////////////////////////////////////////////////
}
