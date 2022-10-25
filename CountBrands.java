/*
Description:	This algorithm counts the number of times category codes appear in conjunction with smartphones
Input:		Full dataset as .csv
		List of session ID's
Output:		Filtered dataset
*/

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountBrands {
  
	public static class CountBrandsMapper 
	extends Mapper<LongWritable, Text, Text, IntWritable> {

		private static String filterString = "electronics.smartphone";
		private static IntWritable one = new IntWritable(1);
				
		public void map(LongWritable key, Text value, Context context
			) throws IOException, InterruptedException  {
			
			if (key.get() == 0 && value.toString().contains("event_time")) //Ensures we do not try to process header row from original input file
				return;
			else {			
				String valueString = value.toString(); //Gets entire row as string
				String[] singleEvent = valueString.split(","); //Splits row into columns
				if (!singleEvent[4].matches(filterString)){ //If the category does not match the filter, we output
					context.write(new Text(singleEvent[5]), one); //Write key-value pair as output
				}
			}		
		}
	}

	public static class CountBrandsReducer 
	extends Reducer<Text, IntWritable, Text, IntWritable> {
	
		private IntWritable result = new IntWritable(1);

		public void reduce(Text key, Iterable<IntWritable> values,Context context
			) throws IOException, InterruptedException  {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CountCategoryCodes");
		job.setJarByClass(CountBrands.class);
		job.setMapperClass(CountBrandsMapper.class);
		job.setCombinerClass(CountBrandsReducer.class);
		job.setReducerClass(CountBrandsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
