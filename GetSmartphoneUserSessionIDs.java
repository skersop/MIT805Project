/*
Description:	This algorithm finds all user session id's in which interactions with products categorised as smartphones occurred
Input:		Full data set as .csv
Output:		List of user session id's
*/

import java.io.IOException;
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

public class GetSmartphoneUserSessionIDs {
  
	public static class GetSmartphoneUserSessionIDsMapper 
	extends Mapper<LongWritable, Text, Text, NullWritable> {

		private static String filterString = "electronics.smartphone"; //The desired filter
		
		public void map(LongWritable key, Text value, Context context
			) throws IOException, InterruptedException  {
			
			if (key.get() == 0 && value.toString().contains("event_time")) //Ensures we do not try to process header row from original input file
				return;
			else {			
				String valueString = value.toString(); //Gets entire row as string
				String[] singleEvent = valueString.split(","); //Splits row into columns
				if (singleEvent[5].matches(filterString)){ //If the category matches the filter, we output
					context.write(new Text(singleEvent[9]), NullWritable.get()); //Write key-value pair as output
				}
			}		
		}
	}
	
	public static class GetSmartphoneUserSessionIDsReducer
		extends Reducer<Text, NullWritable, Text, NullWritable> {
		
		//Reducer will automatically remove duplicates
		public void reduce(Text key, Iterable<NullWritable> values, Context context) 
		throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "filterSmartphoneUserSessions");
		job.setJarByClass(GetSmartphoneUserSessionIDs.class);
		job.setMapperClass(GetSmartphoneUserSessionIDsMapper.class);
		job.setCombinerClass(GetSmartphoneUserSessionIDsReducer.class);
		job.setReducerClass(GetSmartphoneUserSessionIDsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
