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

import jdk.nashorn.api.tree.ForInLoopTree;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class ReadImports{
  private String title;
private String tableName;
public String fields;
public ReadImports(){

}

public void customExport(String query, String name, ArrayList<String> feildNames)throws DLException{
  Booth booth = new Booth();
  booth.customExport(query, name, feildNames);
}

public void runBoothFiles(String totalName) throws DLException{
  
    File folder = new File("./Import/Booths");
    File[] listOfFiles = folder.listFiles();

    Booth booth = new Booth();
    for (int i = 0; i < listOfFiles.length; i++) {
        if (listOfFiles[i].isFile()) {
          booth.runBooth(listOfFiles[i].getName(), tableName);
        } 
    }  

    booth.totalRegister(totalName);
  
  
}

public void runLoginFiles(String date[], String title) throws DLException{
  
  File folder = new File("./Import/Login");
  File[] listOfFiles = folder.listFiles();
  Logs log = new Logs();
  log.title = title;
  log.dates = date;
  for (int i = 0; i < listOfFiles.length; i++) {
      if (listOfFiles[i].isFile()) {
       // System.out.println(listOfFiles[i].getName());
        log.runLogin(listOfFiles[i].getName(), tableName);
      } 
  }  
  ArrayList<ArrayList<String>>  eventGroup = log.getEventGroup();
log.exportFile(eventGroup);


}

public void runRegister() throws DLException{
  File folder = new File("./Import/Register");
  File[] listOfFiles = folder.listFiles();

  for (int i = 0; i < listOfFiles.length; i++) {
      if (listOfFiles[i].isFile()) {
        
       // System.out.println(listOfFiles[i].getName());
      runData(listOfFiles[i].getName());
      } 
  }  
  
}

public void runData(String fileName) throws DLException{
  ArrayList<RegBook> rows = readRegFromCSV(fileName);

  //System.out.println(rows.get(0));
  
RegBook feilds = rows.get(0);
createTable(feilds);
for(int x = 0;  x < rows.size(); x++){
rows.get(x).insert(tableName);
}


}

public ArrayList<RegBook> readRegFromCSV(String fileName) {


  title = fileName;
  tableName = title.substring(0, title.indexOf(".csv"));
tableName = tableName.replace('-', '_');
if(tableName.length() > 30)
tableName = tableName.substring(0, 30);

ArrayList<String> columnNames = new ArrayList<>();

  String filePath = "Import\\Register\\" + fileName;

  
     
      Path pathToFile = Paths.get(filePath);
    
      ArrayList<RegBook> rbook = new ArrayList<>();
      // create an instance of BufferedReader
      // using try with resource, Java 7 feature to close resources
      try (BufferedReader br = Files.newBufferedReader(pathToFile, StandardCharsets.UTF_8)) {
              
          // read the first line from the text file
          String line = br.readLine(); 
          // loop until all lines are read
          int x = 0;
          while (line != null) {
    
              // use string.split to load a string array with the values from
              // each line of
              // the file, using a comma as the delimiter
              String[] attributes = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
             
              ArrayList<String> tempArray = new ArrayList<String>(); 
            
              for (String string : attributes) {
              
                tempArray.add(string);
              }
             if(x == 0){
              
               for (String col : tempArray) {
                    if(col.length() > 30){
                      col = col.substring(0, 30);
                    }
                  col =  col.replace('"', ' ');
                   columnNames.add(col);
                   }

                   //System.out.println(columnNames);
             }
             else{
              if(tempArray.size() < columnNames.size()){
                int size = columnNames.size() - tempArray.size();
                for(int y = 0; y < size; y++){
                  tempArray.add(" ");
                }
              }


              rbook.add(new RegBook(tempArray, columnNames));
             }
            
             
                 // adding book into ArrayList
               
                

              x+= 1;
             
              // read next line before looping
              // if end of file reached, line would be null
              line = br.readLine();
          }

  
      } 
      catch (IOException ioe) {
          ioe.printStackTrace();
      }
   
      return rbook;
  }

 

public void createTable(RegBook book)throws DLException{
  MySQLDatabase db = connect();
   db.setData("DROP TABLE IF EXISTS " + tableName + ";");

    String createQuery = "CREATE TABLE " + tableName + " (";



for(int x = 0; x < book.getColumnName().size(); x++){
  if(x == 0 ){
    createQuery += book.getColumnName().get(x) + " DATE";
  }
  else if( x < 3){
    createQuery += ", " + book.getColumnName().get(x) + " VARCHAR(30)";
  }
  else
  createQuery += ", " + book.getColumnName().get(x) + " VARCHAR(200)";
}
    createQuery += ");";


    db.setData(createQuery);
    db.close();
}

private MySQLDatabase  connect() throws DLException {
    MySQLDatabase db = new MySQLDatabase();
    db.connect();
    return db;
 }
}
class RegBook{
private ArrayList<String> attributes;
private ArrayList<String> columnName;

public RegBook(ArrayList<String> attr, ArrayList<String> columnName){
  this.attributes = attr;
  this.columnName = columnName;
}
/**
 * @return the attributes
 */
public ArrayList<String> getAttributes() {
  for(int x = 0; x < attributes.size(); x++){
    attributes.set(x, attributes.get(x).replace("'", " "));
  }
  return attributes;
}

/**
 * @return the columnName
 */
public ArrayList<String> getColumnName() {


  for(int x = 0; x < columnName.size(); x++){
    columnName.set(x, columnName.get(x).replaceAll("[^a-zA-Z0-9]", " "));
    columnName.set(x, columnName.get(x).replace(' ', '_'));
  }
  return columnName;
}

public void insert(String name)throws DLException{
  MySQLDatabase db = connect();
  String querey = "INSERT INTO " + name + " (";
boolean start = true;
  for (String string : getColumnName()) {
    if(!start){
      querey+= ", ";
   
    }
  querey+= string;
   
    start = false;

  }
  querey += ") VALUES (";
  start = true;
  for (String string : getAttributes()) {
    if(!start)
    querey+= ", '" + string + "'";
else{

  querey+=  "STR_TO_DATE('" + string.replace('/', '-') + "', '%m-%d-%Y')";
}
  
    start = false;
  }
  querey+= ");";
// System.out.println(toString());
//System.out.println(querey);
  db.setData(querey);
db.close();
}



@Override
public String toString() {
  String values = "[";
  for (String string : attributes) {
    values += string + " Next: ";
  }
  values += "]";
    return values;
}
private MySQLDatabase  connect() throws DLException {
  MySQLDatabase db = new MySQLDatabase();
  db.connect();
  return db;
}
}