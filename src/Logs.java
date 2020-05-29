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




public class Logs {
    public String title;
    public String[] dates;
    private String eventTable;
    
    public void runLogin(String location, String event) throws DLException {
        eventTable = event;
        List<LogRows> books = readBooksFromCSV(location, "none");
    
        // let's print all the person read from CSV file
        for (LogRows b : books) {
//System.out.println(b.toString());
          b.insert();
        }
    
      

    
   // exportFile();
    }
    
    
    public List<LogRows> readBooksFromCSV(String fileName, String event) {
    
    
  
   
    
    String filePath = "Import\\Login\\" + fileName;
    
    
    
        List<LogRows> books = new ArrayList<>();
        Path pathToFile = Paths.get(filePath);
    
        // create an instance of BufferedReader
        // using try with resource, Java 7 feature to close resources
        try (BufferedReader br = Files.newBufferedReader(pathToFile,
        StandardCharsets.UTF_8)) {
    
            // read the first line from the text file
            String line = br.readLine(); 
            // loop until all lines are read
            int x = 0;
            while (line != null) {
    
                // use string.split to load a string array with the values from
                // each line of
                // the file, using a comma as the delimiter
                if(x > 0){
                    String[] attributes = line.split(",");
                 //   System.out.println(attributes[2]);  
                    LogRows book = createBook(attributes, event);
        
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
    
    public LogRows createBook(String[] metadata, String eventName) {

        String date = metadata[0].replaceAll("\"","");
        String email = metadata[1].replaceAll("\"","");
      
  
        // create and return book of this metadata
        return new LogRows(date, email, eventName);
    }
    
    
    public  ArrayList<ArrayList<String>> getEventGroup()throws DLException{
        MySQLDatabase db = connect(); 
String query = "SELECT DISTINCT Login.DATE_SUBMITTED, Login.EMAIL FROM " + eventTable + " INNER JOIN LOGIN ON Concat(" + eventTable + 
".First_Name,' ' , " + eventTable + ".Last_Name) = Login.Email OR " + eventTable + ".Email = Login.Email WHERE ";
  

String query2 = "UNION SELECT DISTINCT  DATE_SUBMITTED, EMAIL FROM " + eventTable + " WHERE ";

for(int x = 0; x < dates.length; x++){
    query += "Login.Date_Submitted = '" + dates[x] + "'";
    query2 += "Date_Submitted = '" + dates[x] + "'";
    if(x != dates.length - 1){
        query += " OR ";
        query2 += " OR ";
    }
    
}
query2 += ";";
query +=  query2;


          ArrayList<ArrayList<String>> group = db.getData(query); 
      //System.out.println(query);
   


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
    
  
   public void updateEvent(ArrayList<ArrayList<String>> dataRow)throws DLException{
    MySQLDatabase db = connect(); 
    for (ArrayList<String> row : dataRow) {
      //  System.out.println("UPDATE LOGIN SET EVENT = '" + eventTable + "' WHERE DATE_SUBMITTED = '" + row.get(0) + "' AND EMAIL = '" + row.get(1) + "';");
      db.setData("UPDATE LOGIN SET EVENT = '" + eventTable + "' WHERE DATE_SUBMITTED = '" + row.get(0) + "' AND EMAIL = '" + row.get(1) + "';"); 
    }
      
    db.close();

   }
    
  
    public void exportFile( ArrayList<ArrayList<String>> dataRow )throws DLException{
    
     try { 
       // ArrayList<ArrayList<String>> row = compareTables();
        FileWriter writer = new FileWriter("Exports\\Login\\" + title + ".csv");
    // create CSVWriter with '|' as separator 
    CSVWriter csvWriter = new CSVWriter(writer);
     

    updateEvent(dataRow);


    List<String[]> finalData = new ArrayList<String[]>();
    
    ArrayList<String> tempArray = new ArrayList<>();
    tempArray.add("Date_Submitted");   tempArray.add("Email/Username");
    finalData.add(tempArray.toArray(new String[tempArray.size()]));
    
    for (ArrayList<String> rowDatas : dataRow) {
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


class LogRows {
  private String date;
  private String email;
  private String event;
    
    public LogRows(String date, String email, String event) {
        this.date = date;
        this.email = email;
        this.event = event;
    }
    
    public ArrayList<String> getRows(){
        ArrayList<String> list = new ArrayList<String>();
     
    list.add("STR_TO_DATE('" + date.replace('/', '-') + "', '%m-%d-%Y')");
    list.add("'" + email + "'");
    list.add("'" + event  + "'");
        return list;
    }
    private MySQLDatabase  connect() throws DLException {
        MySQLDatabase db = new MySQLDatabase();
        db.connect();
        return db;
     }
    
     public void insert()throws DLException{
        MySQLDatabase db = connect();
      // System.out.println("INSERT INTO LOGIN (Date_Submitted, email, event) SELECT * FROM ( SELECT " + getRows().get(0) + "," + getRows().get(1) + "," + getRows().get(2) + ") AS tmp" + 
      // " WHERE NOT EXISTS (SELECT Date_Submitted, email FROM LOGIN WHERE Date_Submitted = " + getRows().get(0) + " AND EMAIL = " + getRows().get(1) + " );");


        db.setData(" INSERT INTO LOGIN (Date_Submitted, email, event) SELECT * FROM ( SELECT " + getRows().get(0) + "," + getRows().get(1) + "," + getRows().get(2) + ") AS tmp" + 
        " WHERE NOT EXISTS (SELECT Date_Submitted, email FROM LOGIN WHERE Date_Submitted = " + getRows().get(0) + " AND EMAIL = " + getRows().get(1) + " );");
    db.close();
     }
    
    
    @Override
    public String toString() {
        return "LogRow [date =" + date + ", Email =" + email + ", event =" + event + "]";
    }
}