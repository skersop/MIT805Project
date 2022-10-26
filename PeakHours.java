import java.io.IOException;
import java.util.StringTokenizer;
import java.io.File;  // Import the File class
import java.io.FileNotFoundException;  // Import this class to handle errors
import java.util.Scanner; // Import the Scanner class to read text files

public class GetMax {
 
	public static void main(String[] args) throws Exception {
		try{
			File myObj = new File("smartphonetimes.txt");
			Scanner myReader = new Scanner(myObj);				
			String valueString = value.toString(); //Gets entire row as string
			String[] singleEvent = valueString.split(","); //Splits row into columns
			while (myReader.hasNextLine()) {
				filterString = myReader.nextLine();
				if (singleEvent[8].matches(filterString)){ //If the session ID the filter, we output
					context.write(value, NullWritable.get()); //Write key-value pair as output
					break;
				}
			}
			myReader.close();
		} catch (FileNotFoundException e) {
		      	System.out.println("An error occurred.");
		      	e.printStackTrace();
	    	}
	}
}
