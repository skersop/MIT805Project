/*
Description:	Reads list of events per hour, retrieves two hours with highest number of events
Output:		.txt containing thw two hours of the day with the highest number of events
*/
import java.io.IOException;
import java.util.StringTokenizer;
import java.io.File;  
import java.io.FileNotFoundException;  
import java.util.Scanner; 
import java.io.File;
import java.io.FileWriter;

public class PeakHours{
 
	public static void main(String[] args) throws Exception {
		try{
			//Initialise variables
			int highestIndex = 0;
			int highestValue = 0;
			int highestIndex2 = 0;
			int highestValue2 = 0;
			
			//Import data
			File myObj = new File("SmartphoneTimes.txt");
			Scanner myReader = new Scanner(myObj);				
			while (myReader.hasNextLine()) {
				String value = myReader.nextLine();
				String[] valueSplit = value.split("\\t"); //Splits row into columns
				if(Integer.parseInt(valueSplit[1]) > highestValue){ //If read value is highest value seen, save as highest value and shift highest value to second highest
					highestIndex2 = highestIndex;
					highestValue2 = highestValue;					
					highestIndex = Integer.parseInt(valueSplit[0]);
					highestValue = Integer.parseInt(valueSplit[1]);
				} else if(Integer.parseInt(valueSplit[1]) > highestValue2){//If read value is second highest value seen, save as second highest value highest
					highestIndex2 = Integer.parseInt(valueSplit[0]);
					highestValue2 = Integer.parseInt(valueSplit[1]);
				}
			}
			myReader.close();

			//Create output file if it doesn't exist
			File outFile = new File("PeakHours.txt");		      
			if (outFile.createNewFile()) {
				System.out.println("File created: " + outFile.getName());
			} else {
				System.out.println("File already exists.");
			}
				
			// Write results
	      		FileWriter myWriter = new FileWriter("PeakHours.txt");
	      		System.out.println("Peak hour 1: " + highestIndex);
	      		System.out.println("Peak hour 2: " + highestIndex2);
	      		myWriter.write(Integer.toString(highestIndex));
	      		myWriter.write("\n");
	      		myWriter.write(Integer.toString(highestIndex2));
	      		myWriter.close();
	      		      		
		} catch (FileNotFoundException e) {
		      	System.out.println("An error occurred.");
		      	e.printStackTrace();
	    	}
	}
}
