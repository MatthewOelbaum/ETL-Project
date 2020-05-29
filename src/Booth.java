import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.io.*;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Booth{

private String title;
private String currentBooth;
private String eventTable;

public void runBooth(String location, String event) throws DLException {
    eventTable = event;
    List<Rows> books = readBooksFromCSV(location);

    // let's print all the person read from CSV file
    for (Rows b : books) {
      b.insert();
    }

exportFile();
exportFileMissing();
}


public List<Rows> readBooksFromCSV(String fileName) {


title = fileName;
currentBooth = title.substring(0, title.indexOf(".csv"));

String filePath = "Import\\Booths\\" + fileName;



    List<Rows> books = new ArrayList<>();
    Path pathToFile = Paths.get(filePath);

    // create an instance of BufferedReader
    // using try with resource, Java 7 feature to close resources
    try (BufferedReader br = Files.newBufferedReader(pathToFile,
            StandardCharsets.US_ASCII)) {

        // read the first line from the text file
        String line = br.readLine(); 
        // loop until all lines are read
        int x = 0;
        while (line != null) {

            // use string.split to load a string array with the values from
            // each line of
            // the file, using a comma as the delimiter
            if(x > 2){
                String[] attributes = line.split(",");
             //   System.out.println(attributes[2]);  
                Rows book = createBook(attributes, currentBooth);
    
                // adding book into ArrayList
                books.add(book);
    
            }
            x+= 1;
           
            // read next line before looping
            // if end of file reached, line would be null
            line = br.readLine();
        }

    } catch (IOException ioe) {
        ioe.printStackTrace();
    }

    return books;
}

public Rows createBook(String[] metadata, String boothName) {
    String name = metadata[0];
    String email = metadata[1];
    String WhenKicked = metadata[2];
    String Min = metadata[3];


    // create and return book of this metadata
    return new Rows(name, email, WhenKicked, Min, boothName);
}


public  ArrayList<ArrayList<String>> compareTables()throws DLException{
    MySQLDatabase db = connect(); 
    String feilds = getColumnNamesQuery(true);  
  
// System.out.println("SELECT DISTINCT " + feilds + " FROM " + eventTable + " INNER JOIN Booth ON Concat(" + eventTable + ".First_Name,' ' , " + eventTable + ".Last_Name) = Booth.ScreenName OR " + eventTable + ".Email = Booth.Email WHERE Booth.BoothName = '" + currentBooth + "'");
      ArrayList<ArrayList<String>> group = db.getData("SELECT DISTINCT " + feilds + " FROM " + eventTable + " INNER JOIN Booth ON Concat(" + eventTable + ".First_Name,' ' , " + eventTable + ".Last_Name) = Booth.ScreenName OR " + eventTable + ".Email = Booth.Email WHERE Booth.BoothName = '" + currentBooth + "'"); 
  
    db.close();
  
    return group;
  
}


public ArrayList<String> exportRowConverter(ArrayList<String> row){
    ArrayList<String> newRow = new ArrayList<String>();
for (String string : row) {
    newRow.add(string + "");
}
return newRow;
}


public void totalRegister(String name) throws DLException{
    String feilds = getColumnNamesQuery(false);  
    MySQLDatabase db = connect(); 
    String query = "SELECT DISTINCT " + feilds +  " ,COUNT( booth.boothName), SUM(booth.minutes) FROM " + eventTable +
     " INNER JOIN Booth ON Concat(" + eventTable + ".First_Name,' ' , " + eventTable + 
     ".Last_Name) = Booth.ScreenName OR " + eventTable + 
     ".Email = Booth.Email GROUP BY " + feilds + ";";

   //  System.out.println(query);
    ArrayList<ArrayList<String>> group = db.getData(query); 

    ArrayList<String> columnNames = getColumnTableNames(false);
    columnNames.add("Total Companies Visited"); columnNames.add("Total Minutes");
    customExport(query, name, columnNames);

    db.close();
}



public  String getColumnNamesQuery( Boolean isMinutes) throws DLException{
    MySQLDatabase db = connect();
    ArrayList<ArrayList<String>> group = db.getData("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + eventTable + "' AND COLUMN_NAME != 'Date_Submitted' AND COLUMN_NAME != 'First_Name' AND COLUMN_NAME != 'Last_Name' AND COLUMN_NAME != 'Full_Name' AND COLUMN_NAME != 'Email' AND COLUMN_NAME != 'BoothName'"); 
  db.close();
  String export = "";

   
    export += "" + eventTable + ".Date_Submitted, " + eventTable + ".First_Name, " + eventTable + ".Last_Name, " + eventTable + ".Email, ";
   
int x = 0;
  for (ArrayList<String> arrayList : group) {
    
    export += "" + eventTable + "." + arrayList.get(0);
if(x != group.size() - 1){
    export+= ", ";
}
    x++;
           
    }

     //may need to change booth. if each booth table has a diffrent name
     if(isMinutes){
        export += ", booth.Minutes";
     }
   
      
  
    return export;
}

public ArrayList<String> getColumnTableNames(Boolean isMin) throws DLException{
    MySQLDatabase db = connect();
    ArrayList<ArrayList<String>> group = db.getData("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + eventTable + "' AND COLUMN_NAME != 'Date_Submitted' AND COLUMN_NAME != 'First_Name' AND COLUMN_NAME != 'Last_Name' AND COLUMN_NAME != 'Full_Name' AND COLUMN_NAME != 'Email' AND COLUMN_NAME != 'BoothName'"); 
  db.close();
  ArrayList<String> export = new ArrayList<String>();


   // export += "Date_Submitted, First_Name, Last_Name, ";
    export.add("Date_Submitted");
    export.add("First_Name");
    export.add("Last_Name");
    export.add("Email");


  for (ArrayList<String> arrayList : group) {
    export.add( arrayList.get(0));
            
    }

     //may need to change booth. if each booth table has a diffrent name
     if(isMin){
        export.add( "Minutes");
     }
   
    
  
    return export;
}


public void customExport(String query, String name,  ArrayList<String> feildNames)throws DLException{{
    try { 
        MySQLDatabase db = connect(); 
        ArrayList<ArrayList<String>> row = db.getData(query);
     
        db.close();

      
        FileWriter writer = new FileWriter("Exports\\Custom\\" + name + ".csv");
    // create CSVWriter with '|' as separator 
    CSVWriter csvWriter = new CSVWriter(writer);
     
    List<String[]> finalData = new ArrayList<String[]>();
   

finalData.add(feildNames.toArray(new String[feildNames.size()]));
    for (ArrayList<String> rowDatas : row) {
        ArrayList<String> tempArrayRow = exportRowConverter(rowDatas);
      finalData.add(tempArrayRow.toArray(new String[tempArrayRow.size()]));
    }
    
    
        csvWriter.writeAll(finalData);
    
        csvWriter.close();
    writer.close();
     }
     catch (IOException e) { 
        // TODO Auto-generated catch block 
        e.printStackTrace(); 
     }
    } 
}

//

public void exportFileMissing()throws DLException{
    try { 
        String query = "SELECT DISTINCT booth.screenname, booth.email, booth.Minutes  FROM " + eventTable + " Right JOIN Booth ON Concat(" +
        eventTable + ".First_Name,' ' ," + eventTable + ".Last_Name) = Booth.ScreenName OR " + eventTable + ".Email = Booth.Email WHERE Booth.BoothName = '" + currentBooth  +
        "' AND " + eventTable + ".DATE_SUBMITTED IS NULL;";
        MySQLDatabase db = connect();
        
        ArrayList<ArrayList<String>> row = db.getData(query);
        db.close();
        FileWriter writer = new FileWriter("Exports\\Students\\Rejected\\Rejected_" + title);
    // create CSVWriter with '|' as separator 
    CSVWriter csvWriter = new CSVWriter(writer);
     
    List<String[]> finalData = new ArrayList<String[]>();
    
    ArrayList<String> tempArray = new ArrayList<>();
    tempArray.add("Screen Name");
    tempArray.add("Email");
    tempArray.add("Minutes");
    finalData.add(tempArray.toArray(new String[tempArray.size()]));
    
    for (ArrayList<String> rowDatas : row) {
        ArrayList<String> tempArrayRow = exportRowConverter(rowDatas);
      finalData.add(tempArrayRow.toArray(new String[tempArrayRow.size()]));
    }
    
    
        csvWriter.writeAll(finalData);
    
        csvWriter.close();
    writer.close();

     }
     catch (IOException e) { 
        // TODO Auto-generated catch block 
        e.printStackTrace(); 
    } 
}

public void exportFile( )throws DLException{

 try { 
    ArrayList<ArrayList<String>> row = compareTables();
    FileWriter writer = new FileWriter("Exports\\Students\\" + title);
// create CSVWriter with '|' as separator 
CSVWriter csvWriter = new CSVWriter(writer);
 
List<String[]> finalData = new ArrayList<String[]>();

ArrayList<String> tempArray = getColumnTableNames(true);
finalData.add(tempArray.toArray(new String[tempArray.size()]));

for (ArrayList<String> rowDatas : row) {
    ArrayList<String> tempArrayRow = exportRowConverter(rowDatas);
  finalData.add(tempArrayRow.toArray(new String[tempArrayRow.size()]));
}


    csvWriter.writeAll(finalData);

    csvWriter.close();
writer.close();
 }
 catch (IOException e) { 
    // TODO Auto-generated catch block 
    e.printStackTrace(); 
} 
}

private MySQLDatabase  connect() throws DLException {
    MySQLDatabase db = new MySQLDatabase();
    db.connect();
    return db;
 }
}

class Rows {
private String ScreenName;
private String Email;
private String WhenKicked;
private String Minute;
private String BoothName;

public Rows(String ScreenName, String Email, String WhenKicked, String Minute, String boName) {
    this.ScreenName = ScreenName;
    this.Email = Email;
    this.WhenKicked = WhenKicked;
    this.Minute = Minute;
    this.BoothName = boName;
}

public ArrayList<String> getRows(){
    ArrayList<String> list = new ArrayList<String>();
list.add(ScreenName);
list.add(Email);
list.add(Minute);
list.add(BoothName);
    return list;
}
private MySQLDatabase  connect() throws DLException {
    MySQLDatabase db = new MySQLDatabase();
    db.connect();
    return db;
 }

 public void insert()throws DLException{
    MySQLDatabase db = connect();
   // System.out.println("INSERT INTO BOOTH (ScreenName, Email, Minutes, BoothName) VALUES (?,?,?,?);");
   // System.out.println(getRows());
    db.setData("INSERT INTO BOOTH (ScreenName, Email, Minutes, BoothName) VALUES (?,?,?,?);", getRows());
db.close();
 }


@Override
public String toString() {
    return "Book [name=" + ScreenName + ", Email =" +  Email + ", WhenKicked=" + WhenKicked + ", Minute =" +  Minute
            + "]";
}

}
