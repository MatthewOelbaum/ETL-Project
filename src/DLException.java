import java.sql.*;
import javax.sql.*;
import java.util.*;
import java.io.*;


public class DLException extends Exception {
   
   // All attributes are created  
   Exception exception = null;
   String message = null;
   String output = null;
   File file = new File("ErrorLog.txt");
   BufferedWriter bw = null;
   PrintWriter pw = null;
   
   
   /**
    * No parameters are used
    * 
    * @see #super
    * @see #writeLog
    *
    */
    
   public DLException() {
      super();
      writeLog();
   }
   
    /**
    * 
    * @param message of showing error message
    * @see #super
    * @see #writeLog
    * 
    */

   public DLException(String message) {
      super(message);
      writeLog();
   }
   
    /**
    * 
    * @param _exception is to catch any error
    * @see #exception
    * @see #writeLog
    * 
    */
   
   public DLException(Exception _exception) {
      exception = _exception;
      writeLog();
   }
   
    /**
    * 
    * @param _exception is to catch any error
    * @para _message is used to show a message error
    * @see #exception
    * @see #writeLog
    * 
    */
   
   public DLException(Exception _exception, String _message) {
      exception = _exception;
      message = _message;
      writeLog();
   }
   
   // writeLog method
   public void writeLog() {  
      // Try and catch statment   
      try {
         if(!file.exists()) {
            file.createNewFile();
         }
         
         // write character-oriented data to a file
         FileWriter fw = new FileWriter(file, true);
         bw = new BufferedWriter(fw);
         pw = new PrintWriter(bw);
         
         // Shows what error occurs
         if (exception.getCause() == null) {
            pw.println("Reason: Unknown - please refer to Cause below." + "\nCause: " + message + "\n");
            exception.printStackTrace(pw);
         }
         
         // Shows what error occurs
         else {
            pw.println("Reason: " + exception.getCause() + "\nCause: " + message + "\n");
         }
         
         // Close PrintWriter
         pw.flush();
         pw.close();
      }
      
      // Shows why program is not working
      catch(Exception e) {
         System.out.println("ERROR: " + e.toString());
         System.exit(-1);
      }
   }
}
