package bayes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class NBModelEvaluationReducer extends MapReduceBase implements Reducer<NullWritable, Text, Text,Text>{
        
    @SuppressWarnings("unused")
	private final static long zero = 0;
	HashMap<String, Long> h;
	////////////////////////////////////////////////////////////////	
	@Override
	 public void configure(JobConf conf){
		// 
	    }

	
	////////////////////////////////////////////////////////////////
	// NullWritable, Text
	public void reduce(NullWritable key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporte) throws IOException {
		// TODO Auto-generated method stub
		h = new HashMap<String, Long>();
		Long count;
		String ky;
		
		// it ensures to set string as key in hashmap
		while(values.hasNext())               
        {
        	ky = values.next().toString().trim();    
            count = h.get(ky);
            h.put(ky, (count == null) ? 1 : count + 1);
        }	
                
        // iterate through all of the actual and identified classes        
        for (Map.Entry<String, Long> entry : h.entrySet()) {
            ky = entry.getKey().toString();
            count = entry.getValue();
       //   output.collect(new Text (ky), new Text (count.toString()));
        }
        String out = DeviseConfusionMatric.Draw(h);
        DeviseConfusionMatric.Print_Screen(out);
        output.collect(new Text ( out), new Text(""));
		
        //long ret = (long)h.size();
	    //output.collect(new Text ( h.toString()), new Text( ((Long) ret).toString()));
	   
	}
	////////////////////////////////////////////////////////////////
	
}


