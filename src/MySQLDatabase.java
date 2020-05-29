import java.sql.*;
import javax.sql.*;
import java.util.*;
import java.io.*;

 


public class MySQLDatabase {

   // Two attributes are created but they are constant
   private static String EXCEPTIONMESSAGE = "Failure to perform operation";
   private static String SQLEXCEPTIONMESSAGE = "SQL Engine Failure";


   // All attributes are created
   private Connection connection; // Database Connection Object
   private boolean inTrans; // Whether or not we are currently in a transaction
   public String driver = "com.mysql.jdbc.Driver";
   public String username = "root";
   public String password = "student"; // Needs to be changed to your password
   public String dbServer = "localhost";
   public String dbName = "JobsConnected";

   public MySQLDatabase() {}

    
   public MySQLDatabase(String username, String password, String dbServer, String dbName) {
      this.username = username;
      this.password = password;
      this.dbServer = dbServer;
      this.dbName = dbName;
   
      inTrans = false;
   }

    
   public Connection getConnection() {
      return this.connection;
   }

   
   public boolean connect() throws DLException {
   
      //if in transaction, CAN NOT connect
      if ( inTrans ) {
         return false;
      }

      String connectionURI = "jdbc:mysql://" 
                                + dbServer + ":3306" 
                                + "/" + dbName + "?autoReconnect=true&useSSL=false" 
                                + "&user=" + username 
                                + "&password=" + password;
   
      try {
         // To set the driver
         Class.forName(driver);
         // To get it connected
         connection = DriverManager.getConnection(connectionURI);
      } 
      // To catch the error and show message
      catch(Exception e) {
         throw new DLException(e, e.toString());
      }
      // Show that connection is successful 
      return true; 
   }


   public boolean close() throws DLException {
   
      //if in transaction, CAN NOT close
      if ( inTrans ) {
         return false;
      }
   
      try {
         // The connection is closed
         connection.close();
      } 
      // To catch the error and show message
      catch (Exception e) {
         throw new DLException(e, e.toString());
      }
      // Shows that connection is closed successfully
      return true; 
      }
   

   public static boolean main() throws DLException {
      
      // Instantiates this database
      MySQLDatabase MySQL = new MySQLDatabase(); 
      
      // Opens both databases below but prints some information about drivers and others.
      System.out.println( "\n");
      System.out.println("MySQLDatabase Connection: " + MySQL.connect() + " \nDriver Loaded: " + MySQL.driver + "\nConnecting to the database: " + MySQL.dbName);  
      System.out.println( "\n");
      
      // return false
      return false;
   } 
   

  
   public ArrayList<ArrayList<String>> getData(String query) throws DLException {

   
      // To set up the array named result
      ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>();
      // To set up the ResultSet named rs
      ResultSet rs;
   
   
      try {
         // To set up the statement with connection for query
         Statement statement = connection.createStatement();
         rs = statement.executeQuery(query);
         ResultSetMetaData rsmd = rs.getMetaData();
         int numCols = rsmd.getColumnCount();
      
         // While there are more rows, add row to result
         while(rs.next()) {
            ArrayList<String> row = new ArrayList<String>();
         
            // Process row
            for(int i = 1; i <= numCols; i++) {
               String res = rs.getString(i);
               row.add(res);
            }
            // Added row to the result
            result.add(row);
         }
      
      } 
      // To catch the error and show message
      catch (Exception e) {
         throw new DLException(e, EXCEPTIONMESSAGE);
      }
      // return
      return result;
   
   }

  

   public ArrayList<ArrayList<String>> getProcedure(String query, int data) throws DLException{

 // To set up the ResultSet named rs
 ResultSet rs;
 ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>();
 try {
    // Set up the statement with connection 
    CallableStatement statement = connection.prepareCall(query);
    statement.setInt(1, data);
    rs = statement.executeQuery();
    ResultSetMetaData rsmd = rs.getMetaData();
    int numCols = rsmd.getColumnCount();
 
    // While there are more rows, add row to result
    while(rs.next()) {
       ArrayList<String> row = new ArrayList<String>();
    
       // Process row
       for(int i = 1; i <= numCols; i++) {
          String res = rs.getString(i);
          row.add(res);
       }
       // Added row to the result
       result.add(row);
    }
 
 } 
 // To catch the error and show message
 catch (SQLException sqle) {
   throw new DLException(sqle, SQLEXCEPTIONMESSAGE);
} 
// To catch the error and show message
catch (Exception e) {
   throw new DLException(e, EXCEPTIONMESSAGE);
}
// return
return result;

   }


   public ArrayList<ArrayList<String>> getData(String query, List<String> vals) throws DLException {
   
      // To set the attributes and array
      ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>();
      ResultSet rs;
      ResultSetMetaData rsmd;
      int numCols;
   
      try {
         //To set up the prepare statement and insert query to it
         System.out.println("Test");
         System.out.println(query);
         PreparedStatement statement = prepare(query);
      
         // Insert each string value
         for (int i = 0; i < vals.size(); i++) {
            statement.setString(i+1, vals.get(i));
         }
      
         // Perform query
         rs = statement.executeQuery();
         rsmd = rs.getMetaData();
         numCols = rsmd.getColumnCount();
      
         // Add results to ArrayList
         while (rs.next()) {
            ArrayList<String> row = new ArrayList<String>();
         
            // Process row
            for (int i = 1; i <= numCols; i++) {
               row.add(rs.getString(i));
            }
            // Added row to the result
            result.add(row);
         }
      
      } 
      // To catch the error and show message
      catch (SQLException sqle) {
         throw new DLException(sqle, SQLEXCEPTIONMESSAGE);
      } 
      // To catch the error and show message
      catch (Exception e) {
         throw new DLException(e, EXCEPTIONMESSAGE);
      }
      // return
      return result;
   }

 
   public void setData(String updateString) throws DLException {
   
     
   
      try {

       
         // Set up the statement with connection
         Statement statement = connection.createStatement();
         
         // statement is executed with updatedString and put it in numAffected
          statement.executeUpdate(updateString);
      } 
      // To catch the error and show message
      catch (Exception e) {
         System.out.println(e);
         throw new DLException(e, EXCEPTIONMESSAGE);
      }
      // return
    
   
   }

   public int setData(String updateString, List<String> vals) throws DLException {
   
      // return the execute statement with updated SQLString and vals (list)
      return executeStatement(updateString, vals);
   
   }

   private PreparedStatement prepare(String SQLString) throws DLException {
   
      try {
         // return the connection with SQLString (Prepare Statement)
         return connection.prepareStatement(SQLString);
      } 
      // To catch the error and show message
      catch (SQLException sqle) {
         throw new DLException(sqle, EXCEPTIONMESSAGE);
      }
   
   }

 
    
   private int executeStatement(String SQLString, List<String> vals) throws DLException {
     
      // Set attribute 
      int numAffected = -1;
   
      try {
         // Set up the Prepared Statement for SQLString
         PreparedStatement statement = prepare(SQLString);
      
         // Insert each string value
         for(int i = 0; i < vals.size(); i++) {
            statement.setString(i+1, vals.get(i));
         }
      
         // To get it updated
         numAffected = statement.executeUpdate();
      
      } 
      // To catch the error and show message
      catch (SQLException sqle) {
         throw new DLException(sqle, SQLEXCEPTIONMESSAGE);
      }
      // To catch the error and show message
      catch (Exception e) {
         throw new DLException(e, EXCEPTIONMESSAGE);
      }
      // return 
      return numAffected;
   }





   ///If using transactions then change connection so it uses local connection var

   public void startTrans() throws DLException {
      
      // set it to be true
      inTrans = true;
   
      try {
         // The connection is set to false when commits
         connection.setAutoCommit(false);
      
      } 
      // To catch the error and show message
      catch (SQLException sqle) {
         throw new DLException(sqle, SQLEXCEPTIONMESSAGE);
      }
   }

 
   public void endTrans() throws DLException {
   
      try {
         // The connection is commited
         connection.commit();
         // The connection is set to true when commits
         connection.setAutoCommit(true);
      } 
      
      // To catch the error and show message
      catch (SQLException sqle) {
         throw new DLException(sqle, SQLEXCEPTIONMESSAGE);
      }
      // Returns false
      inTrans = false;
   }


   public void rollbackTrans() throws DLException {
   
      try {
         // connection is rollback
         connection.rollback();
         // connection is set to true when commited 
         connection.setAutoCommit(true);
      } 
      // To catch the error and show message
      catch (SQLException sqle) {
         throw new DLException(sqle, SQLEXCEPTIONMESSAGE);
      }
      // Returns false
      inTrans = false;
   }
}
   
   

