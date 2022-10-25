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

public class CategoriesPerBrand{

	public static class CategoriesPerBrandMapper
		extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private static IntWritable one = new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {		
		
			String valueString = value.toString(); //Gets entire row as string
			String[] SingleInstanceData = valueString.split(","); //Splits row into columns	
			context.write(new Text(SingleInstanceData[0]), one); 	
		}
	}
	
	public static class CategoriesPerBrandReducer
		extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
		throws IOException, InterruptedException {
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
		Job job = Job.getInstance(conf, "categories per brand");
		job.setJarByClass(CategoriesPerBrand.class);
		job.setMapperClass(CategoriesPerBrandMapper.class);
		job.setCombinerClass(CategoriesPerBrandReducer.class);
		job.setReducerClass(CategoriesPerBrandReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
