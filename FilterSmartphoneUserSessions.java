/*
Description:	This algorithm filters on user sessions corresponding to user session ID's extracted in getSmartphoneUserSessionIDs
Input:		Full dataset as .csv
		List of session ID's
Output:		Filtered dataset
*/

import java.io.IOException;
import java.io.File;  
import java.io.FileNotFoundException;  
import java.util.Scanner; 
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FilterSmartphoneUserSessions {
  
	public static class FilterSmartphoneUserSessionsMapper 
	extends Mapper<LongWritable, Text, Text, NullWritable> {
	
		private HashMap<String, String> sessionIDtoID = new HashMap<String, String>(); // Initialise hashmap
		
		public void setup(Context context) 
			throws IOException, InterruptedException {

			try{
				//Read user session to filter on
				File myObj = new File("GetSmartphoneUserSessionIDs.txt");
				Scanner myReader = new Scanner(myObj);			
				
				while (myReader.hasNextLine()) {
					String sessionID = myReader.nextLine();
					sessionIDtoID.put(sessionID, sessionID); //Map session in hashmap
				}
				
				myReader.close();
			} catch (FileNotFoundException e) {
			      	System.out.println("An error occurred.");
			      	e.printStackTrace();
		    	}


		}

		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
			
			String valueString = value.toString(); //Gets entire row as string
			String[] singleEvent = valueString.split(","); //Splits row into columns
			
			String sessionID = singleEvent[8]; //Get user session ID
			String joinVal = sessionIDtoID.get(sessionID); //Check whether the session ID is in the hashmap 
			// If the session is not null, i.e. present in the hashmap, then output
			if (joinVal != null) {
				context.write(value, NullWritable.get());
			} 
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "filterSmartphoneUserSessions");
		job.setJarByClass(FilterSmartphoneUserSessions.class);
		job.setMapperClass(FilterSmartphoneUserSessionsMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
