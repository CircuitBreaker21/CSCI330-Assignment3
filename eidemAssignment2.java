/* 
This is a Java skeleton code to help you out how to start this assignment.
Please keep in mind that this is NOT a compilable/runnable java file.
Please feel free to use this skeleton code.
Please give a closer look at the "To Do" parts of this file. You may get an idea of how to finish this assignment. 
*/

import java.util.*;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.PrintWriter;

class eidemAssignment2 {
   
   static class StockData {	   
	   // TODO: 
       // Create this class which should contain the information  (date, open price, high price, low price, close price) for a particular ticker
       

       String date;
       double openPrice;
       double highPrice;
       double lowPrice;
       double closePrice;

       StockData(String date, double openPrice, double highPrice, double lowPrice, double closePrice) {
            this.date = date;
            this.openPrice = openPrice;
            this.highPrice = highPrice;
            this.lowPrice = lowPrice;
            this.closePrice = closePrice;
       }

   }
   



   
   static Connection conn;
   static final String prompt = "Enter ticker symbol [start/end dates]: ";
   

   // public static void main(String[] args) throws Exception {
   //    String paramsFile = "readerparams.txt";
   //    if (args.length >= 1) {
   //       paramsFile = args[0];
   //    }
      
   //    Properties connectprops = new Properties();
   //    connectprops.load(new FileInputStream(paramsFile));

   //    try {
   //       Class.forName("com.mysql.jdbc.Driver");
   //       String dburl = connectprops.getProperty("dburl");
   //       String username = connectprops.getProperty("user");
   //       conn = DriverManager.getConnection(dburl, connectprops);
   //       System.out.println("\n\n");
   //       System.out.printf("Database connection %s %s established.%n", dburl, username);
         


   //       //showCompanies();
   //       System.out.println();

   //       Scanner in = new Scanner(System.in);
   //       System.out.print(prompt);
   //       String input = in.nextLine().trim();
         
   //       while (input.length() > 0) {
   //          String[] params = input.split("\\s+");
   //          String ticker = params[0];
   //          String startdate = null, enddate = null;
   //          if (getName(ticker)) {
   //             if (params.length >= 3) {
   //                startdate = params[1];
   //                enddate = params[2];
   //             }               
   //             Deque<StockData> data = getStockData(ticker, startdate, enddate);
   //             System.out.println();
   //             System.out.println("Executing investment strategy");
   //             doStrategy(ticker, data);
   //          } 
            
   //          System.out.println();
   //          System.out.print(prompt);    
   //          input = in.nextLine().trim();
   //       }

   //    // Close the database connection

   //    } catch (SQLException ex) {
   //       System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n",
   //                         ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
   //    }
   // } // End Main





   static void showCompanies() throws SQLException {
      // Create and execute a query
      Statement stmt = conn.createStatement();
      ResultSet results = stmt.executeQuery("select Ticker, Name from company");

      // Show results
      while (results.next()) {
         System.out.printf("%5s %s%n", results.getString("Ticker"), results.getString("Name"));
      }
      stmt.close();
   }// Ends Shows Companies


   static void showTickerDay(String ticker, String date) throws SQLException {
      // Prepare query
      PreparedStatement pstmt = conn.prepareStatement("select OpenPrice, HighPrice, LowPrice, ClosePrice "
            + "  from pricevolume " + "  where Ticker = ? and TransDate = ?");

      // Fill in the blanks
      pstmt.setString(1, ticker);
      pstmt.setString(2, date);
      ResultSet rs = pstmt.executeQuery();

      // Did we get anything? If so, output data.
      if (rs.next()) {
         System.out.printf("Open: %.2f, High: %.2f, Low: %.2f, Close: %.2f%n", rs.getDouble(1), rs.getDouble(2),
               rs.getDouble(3), rs.getDouble(4));
      } else {
         System.out.printf("Ticker %s, Date %s not found.%n", ticker, date);
      }
      pstmt.close();
   } // ENDS showTickerDay


   
   static boolean getName(String ticker) throws SQLException {
	  // TODO:
	  // Execute the first query and print the company name of the ticker user provided (e.g., INTC to Intel Corp.) 
     // Please don't forget to use a prepared statement


      Statement stmt = conn.createStatement();
      ResultSet results = stmt.executeQuery("select Ticker, Name from company");

      // Show results
      String currentTicker;
      String currentName;
      while (results.next()) {
         currentTicker = results.getString("Ticker");
         currentName = results.getString("Name");
         
         if(currentTicker.equals(ticker)){
            System.out.printf( "%s%n", results.getString("Name"));
            stmt.close();
            return true;
         }
      }
      stmt.close();
      System.out.println(ticker.toUpperCase() + " not found in the database\nInput new entry");
      return false;
   } // end getName




   public static boolean isStringNull(String str) {

      // Compare the string with null
      // using == relational operator
      // and return the result
      if (str == null)
         return true;
      else
         return false;
   }

























   static Deque<StockData> getStockData(String ticker, String start, String end) throws SQLException{	  

	  // TODO: 
	  // Execute the second query which will return stock information of the ticker (descending on the transaction date)date
            //SQL injection will be select* from pricevolume where Ticker = 'INTC' order by TransDate DESC  //where 'INTC' is specific ticker


      Statement stmt = conn.createStatement();
      ResultSet results = stmt.executeQuery("select* from pricevolume where Ticker = '" + ticker + "' order by TransDate DESC");

      // Please don't forget to use prepared statement	   
      
      Deque<StockData> result = new ArrayDeque<>();
      
      // TODO: 
      // Loop through all the dates of that company (descendingDouble.parseDouble(results.getString("OpenPrice"))r)
      // Find split if there is any (2:1, 3:1, 3:2) and adjust the split accordingly
      // Include the adjusted data to result (which is a Deque); You can use addFirst method for that purpose
      
      
      //check for splits 
      // NOTE: this is given is DESC order, so the next day is in the previous results 



      boolean first = false; // if hit, turn true
      boolean last = false;


      if (isStringNull(start)){
         last = true;
      } else if(isStringNull(end)){
         last = true;
      } 
      
      
      double scaleFactor = 1;
      double prevOpenPrice = -1;
      int splitCount = 0;
      int tradingDays = 0;
      int scaleCount1 = 0;    //2:1
      int scaleCount2 = 0;    //3:1
      int scaleCount3 = 0;    //3:2
      
      
      while (results.next()) {
         
         String transDate = results.getString("TransDate");
         double openPrice = Double.parseDouble(results.getString("OpenPrice"));
         double closePrice = Double.parseDouble(results.getString("ClosePrice"));
         double lowPrice = Double.parseDouble(results.getString("LowPrice"));
         double highPrice = Double.parseDouble(results.getString("HighPrice"));
         
         
         
         /*TODO
         >need to make the stock data class, then, we que every instance of the results to the deque data structure      
         
         
         initiate new class for each while results, 
         depending on the spits and all, make the proper adjustments
            after the while iteration, the class info should be ready, add that class instance to the deque
            */

             
            if(tradingDays == 0) {
               prevOpenPrice = openPrice;
            }
         
         if(transDate.equals(start)){
            first = true;
         } else if (transDate.equals(end)){
            last = true;
         } else {
            //System.out.println(transDate);
         }
         
         
         if(!first && last){
         
            //System.out.printf("%s %s %f %s %f %n", transDate, "\topen:", openPrice, "\tclose:", closePrice);
            
            //if the abs of this closing price/previous opening price   subtracting the split ratio  is less than split ratio
            double todaysRatio = Double.parseDouble(results.getString("ClosePrice")) / prevOpenPrice;
            
            double adjustedPrice = -1;
            
            if(Math.abs(todaysRatio - 2.0) < 0.2){     
               scaleCount1++;       //2:1 split
               splitCount++;
               adjustedPrice = closePrice/2;
               System.out.println("2:1 split on " + transDate + "\t" + closePrice + "-->" + adjustedPrice);
               
            } else if (Math.abs(todaysRatio - 3.0) < 0.3){    //3:1 split
               scaleCount2++;
               splitCount++;
               adjustedPrice = closePrice / 3;
               System.out.println("3:1 split on " + transDate + "\t" + closePrice + "-->" + adjustedPrice);
               
               
            } else if (Math.abs(todaysRatio - 1.5) < 0.15){   //3:2 split
               scaleCount3++;
               splitCount++;
               adjustedPrice = closePrice /1.5;

               System.out.println("3:2 split on " + transDate + "\t" + closePrice + "-->" + adjustedPrice);
            } 


            //This will set the numbers going to the deQue to the com compounding ratio
            scaleFactor = ((1/ (Math.pow(2, scaleCount1))) * (1 / (Math.pow(3, scaleCount2))) * (1 / (Math.pow(1.5, scaleCount3))));
            openPrice *= scaleFactor;
            highPrice *= scaleFactor;
            lowPrice *= scaleFactor;
            closePrice *= scaleFactor;
         
         
            //Insert to the DeQue
            StockData s = new StockData(transDate, openPrice, highPrice, lowPrice, closePrice);
            result.addFirst(s);
            

            //Next Trading Day 
            prevOpenPrice = Double.parseDouble(results.getString("OpenPrice"));
            tradingDays++;
         } 
      }  //End of reading SQL table


      stmt.close();
      System.out.println(splitCount + " splits in " + tradingDays + " trading days");

      //Retrun the DeQue
      return result;
   } // End getStockData


   
   
   
   static void doStrategy(String ticker, Deque<StockData> data) {
      /*TODO: 
      // Apply Steps 2.6 to 2.10 explained in the assignment description 
      // data (which is a Deque) has all the information (after the split adjustment) you need to apply these steps
      
      get the numbers for the first 50 day closing  average 
      less than 50 days, report net gain of 0 and repeat step two;
      */


      boolean reachedCount = false;

      String date;

      int shares = 0;
      int totalTransactions = 0;
      double cash = 0;
      double currentAverage = 0;
      double openPrice;
      double closePrice;
      double nextOpenPrice;
      double previousClosePrice = 0; //doesnt matter price because first 50 data arent in use 

      ArrayList<Double> movingAverageQue = new ArrayList();   //moving average for closing price
      
      while(!data.isEmpty()){     

         openPrice = data.peek().openPrice;
         closePrice= data.peek().closePrice;
         date = data.peek().date;
         data.pop(); // go to next day
         

         try{
         nextOpenPrice=data.peek().openPrice;
         } catch (Exception e) {
            break;
         }
         
         /*TODO:
         >make a deque for average, 
         
         >if statement of if deque is < 50, go next
         else, remove last, add to fist 
         
         >make a method of how calculating the average 
         */
        
        
        if(movingAverageQue.size() == 50) {
            reachedCount = true;
            
            currentAverage = getAverage(movingAverageQue);
            
            // // buy
            
            // /*
            //  * if closing price is less than the 50 day average, and close price is less
            //  * than open by 3% more (close/open <= 97) buy 100 shares of the stock at price
            //  * open (d+1)
            //  */
            
            if ((closePrice < currentAverage) && ((closePrice / openPrice) <= 0.97000001)) {
               // buy 100 shares
               totalTransactions++;
               cash -= nextOpenPrice * 100;
               cash -=8;
               shares += 100;
               //System.out.println("Buy: " + date + " 100 shares @ " + nextOpenPrice + " total shares = " + shares + ", cash = " + cash + "(average " + currentAverage + ")\n");
            }
            
            // // sell
            if ((shares >= 100) && (openPrice > currentAverage) && (openPrice / previousClosePrice >= 1.00999999)) {
               // sell 100 shares at price (open + close/2)
               totalTransactions++;
               shares -= 100;
               cash += ((openPrice + closePrice) / 2) * 100;
               cash-=8;
               //System.out.println(openPrice + "\t\t" + closePrice + "\t\t" + ((openPrice + closePrice) / 2) * 100);
               //System.out.println("Sell: " + date + " 100 shares @ " + ((openPrice + closePrice) / 2) + " total shares = " + shares + ", cash = " + cash + "(average " + currentAverage + ")\n");   
            }

            
            movingAverageQue.remove(0);
            movingAverageQue.add(closePrice);
         }
         
         if(reachedCount == false){
            movingAverageQue.add(closePrice);
         }


         previousClosePrice = closePrice;
      }
      System.out.println("Transactions executed: " + totalTransactions);
      System.out.println("Net Cash: " + cash);
   }// Ends doStrategy
   
   
         
      static double getAverage(ArrayList<Double> movingAverageQue) {
        
         double total = 0;

         for(int i = 0; i < movingAverageQue.size(); i++){
            total += movingAverageQue.get(i);
         }   
         return total/50;
      } // END getAverage
}//END eidemAssignment2