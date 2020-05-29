import java.sql.*;
import javax.sql.*;
import java.util.*;
import java.io.*;


public class Test3 {
 
    
   public static void main(String[] args) throws DLException {  
 
    
   ReadImports readImports = new ReadImports();


  readImports.runRegister();
  readImports.runBoothFiles("Cecil_Total");
String Date[] = {"2020-05-28"};
   readImports.runLoginFiles(Date, "Cecil_Collage_Event_Logins");


   ArrayList<String> feildNames = new ArrayList<String>();
  




   feildNames.add("Date_Submitted");
   feildNames.add("First_Name");
   feildNames.add("Last_Name");
   feildNames.add("Email");
   feildNames.add("Status");
   feildNames.add("Major");
   feildNames.add("Desired_Industry"); 
   feildNames.add("Desired_Job_Types"); 
   feildNames.add("Are_you_willing_to_relocate?");
   feildNames.add("Do_you_require_accommodations_to_participate_in_this_event?");
   feildNames.add("What_reasonable_accommodations_do_you_require?");

 
 


  //System.out.println(query);
  //readImports.customExport(query, "CustomExports", feildNames);


   }

   
   





} 
      
 

