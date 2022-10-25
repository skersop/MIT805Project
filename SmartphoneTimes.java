import java.io.IOException;
import java.util.StringTokenizer;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SmartphoneTimes {
  
	public static class SmartphoneTimesMapper 
	extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

		//The desired filter
		private static String filterStringCategory = "electronics.smartphone"; 
		//private static String filterStringEvent = "purchase"; 
		private IntWritable one = new IntWritable(1);
		final static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z");
		
		public void map(LongWritable key, Text value, Context context
			) throws IOException, InterruptedException  {
			
			if (key.get() == 0 && value.toString().contains("event_time")) //Ensures we do not try to process header row from original input file
				return;
			else {			
				String valueString = value.toString(); //Gets entire row as string
				String[] singleEvent = valueString.split(","); //Splits row into columns
				if (singleEvent[4].matches(filterStringCategory)){ //If the category matches the filter, we output
					//context.write(new Text(singleEvent[5]), one); //Write key-value pair as output
					ZonedDateTime dateTime = ZonedDateTime.parse(singleEvent[0], formatter);
					context.write(new IntWritable(dateTime.getHour()), one); //Write key-value pair as output
				}
			}		
		}
	}

	public static class SmartphoneTimesReducer 
	extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	
		private IntWritable result = new IntWritable();

		public void reduce(IntWritable key, Iterable<IntWritable> values,Context context
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
		Job job = Job.getInstance(conf, "category mean");
		job.setJarByClass(SmartphoneTimes.class);
		job.setMapperClass(SmartphoneTimesMapper.class);
		job.setReducerClass(SmartphoneTimesReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
