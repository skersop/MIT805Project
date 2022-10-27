import java.io.IOException;
import java.util.StringTokenizer;
import java.io.File;  // Import the File class
import java.io.FileNotFoundException;  // Import this class to handle errors
import java.util.Scanner; // Import the Scanner class to read text files
import java.io.File;
import java.io.FileWriter;

public class PeakHours{
 
	public static void main(String[] args) throws Exception {
		try{
			int highestIndex = 0;
			int highestValue = 0;
			int highestIndex2 = 0;
			int highestValue2 = 0;
			
			File myObj = new File("SmartphoneTimes.txt");
			Scanner myReader = new Scanner(myObj);				
			while (myReader.hasNextLine()) {
				String value = myReader.nextLine();
				String[] valueSplit = value.split("\\t"); //Splits row into columns
				if(Integer.parseInt(valueSplit[1]) > highestValue){
					highestIndex = Integer.parseInt(valueSplit[0]);
					highestValue = Integer.parseInt(valueSplit[1]);
				} else {
					if(Integer.parseInt(valueSplit[1]) > highestValue2){
					highestIndex2 = Integer.parseInt(valueSplit[0]);
					highestValue2 = Integer.parseInt(valueSplit[1]);
						
					}
				}
			}
			myReader.close();

			File outFile = new File("PeakHours.txt");		      
			if (outFile.createNewFile()) {
				System.out.println("File created: " + outFile.getName());
			} else {
				System.out.println("File already exists.");
			}	
			
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
