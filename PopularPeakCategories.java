/*
Description:	Calculates popular non-smartphone categories for hours when smartphone interactions are most prevalent
*/
import java.io.IOException;
import java.util.StringTokenizer;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.io.File;  
import java.io.FileNotFoundException; 
import java.util.Scanner; 
import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;

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

public class PopularPeakCategories {
  
	public static class PopularPeakCategoriesMapper 
	extends Mapper<LongWritable, Text, Text, IntWritable> {
	
		//Initialise variables
		final static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z");
		private static IntWritable one = new IntWritable(1);
		private static String categoryFilter = "electronics.smartphone";		
		private int peakHour;		
		private int peakHour2;	
		private HashMap<String, String> sessionIDtoID = new HashMap<String, String>();
		
		public void setup(Context context) 
			throws IOException, InterruptedException {

			try{
				// Retrieve peak hours
				File peakHourFile = new File("PeakHours.txt");
				Scanner myReader = new Scanner(peakHourFile);				
				peakHour = Integer.parseInt(myReader.nextLine());		
				peakHour2 = Integer.parseInt(myReader.nextLine());		
				myReader.close();	
				
				// Retrieve list of non-smartphone user sessions and map to hashmap
				File smartphoneSessions = new File("GetSmartphoneUserSessionIDs.txt");
				Scanner myReader2 = new Scanner(smartphoneSessions);			
				while (myReader2.hasNextLine()) {
					String sessionID = myReader2.nextLine();
					sessionIDtoID.put(sessionID, sessionID);
				}
				myReader2.close();
			} catch (FileNotFoundException e) {
			      	System.out.println("An error occurred.");
			      	e.printStackTrace();
		    	}
		}
		
		public void map(LongWritable key, Text value, Context context
			) throws IOException, InterruptedException  {
			
			if (key.get() == 0 && value.toString().contains("event_time")) //Ensures we do not try to process header row from original input file
				return;
			else {	
				try{
					String valueString = value.toString(); //Gets entire row as string
					String[] singleEvent = valueString.split(","); //Splits row into columns
					
					int hour = ZonedDateTime.parse(singleEvent[0], formatter).getHour(); //Parse event_time and get the relevant hour

					if((hour == peakHour || hour == peakHour2) && !singleEvent[4].matches(categoryFilter)){ //If a peak hour and not a smartphone-related event
						// If user session does not appear in the hashmap, output
						// Since we want non-smartphone sessions
						// We are performing a left outer join
						String sessionID = singleEvent[8];
						String joinVal = sessionIDtoID.get(sessionID);/ 
						if (joinVal == null) {
							context.write(new Text(singleEvent[3] + " " + singleEvent[4]), one); //Write key-value pair as output
						} 
					}	
								
				} catch (FileNotFoundException e) {
				      	System.out.println("An error occurred.");
				      	e.printStackTrace();
			    	}
			}	
		}
	}

	public static class PopularPeakCategoriesReducer 
	extends Reducer<Text, IntWritable, Text, IntWritable> {
		// Basic count reducer
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
		Job job = Job.getInstance(conf, "PopularPeakCategories");
		job.setJarByClass(PopularPeakCategories.class);
		job.setMapperClass(PopularPeakCategoriesMapper.class);
		job.setReducerClass(PopularPeakCategoriesReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
