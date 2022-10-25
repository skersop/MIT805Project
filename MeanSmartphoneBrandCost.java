import java.io.IOException;
import java.util.StringTokenizer;

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

public class MeanSmartphoneBrandCost {
  
	public static class MeanSmartphoneBrandCostMapper 
	extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		//The desired filter
		private static String filterStringCategory = "electronics.smartphone"; 
		private static String filterStringEvent = "purchase"; 
		private DoubleWritable price = new DoubleWritable();
		
		public void map(LongWritable key, Text value, Context context
			) throws IOException, InterruptedException  {
			
			if (key.get() == 0 && value.toString().contains("event_time")) //Ensures we do not try to process header row from original input file
				return;
			else {			
				String valueString = value.toString(); //Gets entire row as string
				String[] singleEvent = valueString.split(","); //Splits row into columns
				if (singleEvent[4].matches(filterStringCategory) && singleEvent[1].matches(filterStringEvent)){ //If the category matches the filter, we output
					price.set(Float.parseFloat(singleEvent[7])); //Get price and convert to type float
					context.write(new Text(singleEvent[5]), price); //Write key-value pair as output
				}
			}		
		}
	}

	public static class MeanSmartphoneBrandCostReducer 
	extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
		private DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<DoubleWritable> values,Context context
			) throws IOException, InterruptedException  {
			int count = 0;
			double costSum = 0.0;
			for (DoubleWritable val : values) {
				costSum += val.get();
				count++;
			}
			double mean = costSum/count;
			result.set(mean);
			context.write(key, result);
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "category mean");
		job.setJarByClass(MeanSmartphoneBrandCost.class);
		job.setMapperClass(MeanSmartphoneBrandCostMapper.class);
		//job.setCombinerClass(PriceCategoryReducer.class);
		job.setReducerClass(MeanSmartphoneBrandCostReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
