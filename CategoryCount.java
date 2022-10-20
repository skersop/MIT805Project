import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CategoryCount {
  
	public static class SalesMapper 
	extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException  {
			String valueString = value.toString();
			String[] SingleInstanceData = valueString.split(",");
			context.write(new Text(SingleInstanceData[5]), one);
		}
	}



	public static class SalesCountryReducer 
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
		Job job = Job.getInstance(conf, "category count");
		job.setJarByClass(CategoryCount.class);
		job.setMapperClass(SalesMapper.class);
		job.setCombinerClass(SalesCountryReducer.class);
		job.setReducerClass(SalesCountryReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
