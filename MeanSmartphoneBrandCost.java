import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.DataInput; 
import java.io.DataOutput; 

public class MeanSmartphoneBrandCost {
  
  	public static class AverageCountTuple implements Writable {
  		private double average = 0.0;
  		private int count = 0;
  		
  		public int getCount(){
  			return count;
  		}
  		
  		public double getAverage(){
  			return average;
  		}
  		
  		public void setCount(int count){
  			this.count = count;
  		}
  		
  		public void setAverage(double average){
  			this.average = average;
  		}
  		
  		public void readFields(DataInput in) throws IOException {
			average = in.readDouble();
			count = in.readInt();
		}
  			
  		public void write(DataOutput out) throws IOException {
			out.writeDouble(average);
			out.writeInt(count);
		}
			
		public String toString() {
			return average + "\t" + count;
		}
  	}
  
	public static class MeanSmartphoneBrandCostMapper 
	extends Mapper<LongWritable, Text, Text, AverageCountTuple> {

		//The desired filter
		private static String filterStringCategory = "electronics.smartphone"; 
		private static String filterStringEvent = "purchase"; 
		private double price;
		private AverageCountTuple outTuple = new AverageCountTuple();
		
		public void map(LongWritable key, Text value, Context context
			) throws IOException, InterruptedException  {
			
			if (key.get() == 0 && value.toString().contains("event_time")) //Ensures we do not try to process header row from original input file
				return;
			else {			
				String valueString = value.toString(); //Gets entire row as string
				String[] singleEvent = valueString.split(","); //Splits row into columns
				if (singleEvent[4].matches(filterStringCategory) && singleEvent[1].matches(filterStringEvent)){ //If the category matches the filter, we output
					price = Double.parseDouble(singleEvent[6]); //Get price and convert to type float
					outTuple.setAverage(price);
					outTuple.setCount(1);
					context.write(new Text(singleEvent[5]), outTuple); //Write key-value pair as output
				}
			}		
		}
	}

	public static class MeanSmartphoneBrandCostReducer 
	extends Reducer<Text, AverageCountTuple, Text, AverageCountTuple> {
		
		private AverageCountTuple result = new AverageCountTuple();
		
		public void reduce(Text key, Iterable<AverageCountTuple> values, Context context
			) throws IOException, InterruptedException  {
			int count = 0;
			double costSum = 0.0;
			for (AverageCountTuple val : values) {
				costSum += val.getCount() * val.getAverage();
				count += val.getCount();
			}
			
			result.setCount(count);
			result.setAverage(costSum/count);
			
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "category mean");
		job.setJarByClass(MeanSmartphoneBrandCost.class);
		job.setMapperClass(MeanSmartphoneBrandCostMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(AverageCountTuple.class);
		job.setCombinerClass(MeanSmartphoneBrandCostReducer.class);
		job.setReducerClass(MeanSmartphoneBrandCostReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(AverageCountTuple.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
