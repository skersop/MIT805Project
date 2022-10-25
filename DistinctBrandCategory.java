/*
This program retrieves every unique brand-category combination from the input data
*/

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DistinctBrandCategory{

	public static class DistinctBrandCategoryMapper
		extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		private Text outBrandCategory = new Text();
		
		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {		
			if (key.get() == 0 && value.toString().contains("event_time")) //Ensures we do not try to process header row
				return;
			else {			
				String valueString = value.toString(); //Gets entire row as string
				String[] SingleInstanceData = valueString.split(","); //Splits row into columns
				outBrandCategory.set(SingleInstanceData[6] + ',' + SingleInstanceData[4] + ',' + SingleInstanceData[5]); //Concatenate brand and category code and name as key
				context.write(outBrandCategory, NullWritable.get()); //Write key-value pair as output
			}		
		}
	}
  
	public static class DistinctBrandCategoryReducer
		extends Reducer<Text, NullWritable, Text, NullWritable> {
		
		public void reduce(Text key, Iterable<NullWritable> values, Context context) 
		throws IOException, InterruptedException {
			// Write the user's id with a null value
			context.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "distinct brand categories");
		job.setJarByClass(DistinctBrandCategory.class);
		job.setMapperClass(DistinctBrandCategoryMapper.class);
		job.setCombinerClass(DistinctBrandCategoryReducer.class);
		job.setReducerClass(DistinctBrandCategoryReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
