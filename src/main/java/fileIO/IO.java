package fileIO;

import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class IO {

	
	//////////////////////////////////////////////////////////////////////////////
	// move it to one class... it also occurs in schemabuilder
	public void WriteFiles(String Data, String pth) {	
		try {
			File file = new File(pth);
			FileWriter fileWriter = new FileWriter(file);
			fileWriter.write(Data);
			fileWriter.flush();
			fileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/////////////////////////////////////////////////////////////////////////////
	public void purgeDirectory(File dir) {
	    for (File file: dir.listFiles()) {
	        if (file.isDirectory()) purgeDirectory(file);
	        file.delete();
	    }
	}
	
	//////////////////////////////////////////////////////////////////////////////
	 /**
	 * Delete a folder on the HDFS. This is an example of how to interact
	 * with the HDFS using the Java API. You can also interact with it
	 * on the command line, using: hdfs dfs -rm -r /path/to/delete
	 * 
	 * @param conf a Hadoop Configuration object
	 * @param folderPath folder to delete
	 * @throws IOException
	 */
		
	public void deleteFolder(Configuration conf, Path path) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		//Path path = new Path(folderPath); // if folderPath is string argument 
		if(fs.exists(path)) {
			fs.delete(path,true);
		}
	}
	
	////////////////////////////////////////////////////////////////////////////////
    public File[] fileFinder( String dirName, final String extWOdot){
    	File dir = new File(dirName);
    	
    	return dir.listFiles(new FilenameFilter() { 
    	         public boolean accept(File dir, String filename)
    	              { return filename.endsWith(extWOdot); }
    	} );

    } 
	///////////////////////////////////////////////////////////////////////////////


}
