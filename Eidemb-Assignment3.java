/* 
This is a Java skeleton code to help you out how to start this assignment.
Please feel free to use this skeleton code.
Please give a closer look at the "To Do" parts of this file. You may get an idea of how to finish this assignment. 
*/

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.io.*;

class Eidems-Assignment3 {

    static final String defaultReaderParams = "readerparams.txt";
    static final String defaultWriterParams = "writerparams.txt";
    static Connection readerConn = null;   
    static Connection writerConn = null;
	// To Do: Other variables
	    
    static final String getDatesQuery =
        "select max(startDate), min(endDate)" +
        "  from (select Ticker, min(TransDate) as StartDate, max(TransDate) as endDate," +
        "            count(distinct TransDate) as tradingDays" +
        "          from company natural join pricevolume" +
        "          where Industry = ?" +
        "          group by Ticker" +
        "          having tradingDays >= ?) as TickerDates";
    
    static final String getTickerDatesQuery = 
        "select Ticker, min(TransDate) as StartDate, max(TransDate) as endDate," +
        "      count(distinct TransDate) as tradingDays" +
        "  from company natural join pricevolume" +
        "  where Industry = ?" +
        "    and TransDate >= ? and TransDate <= ?" +
        "  group by Ticker" +
        "  having tradingDays >= ?" +
        "  order by Ticker";
        
    static final String getIndustryPriceDataQuery =
        "select Ticker, TransDate, OpenPrice, ClosePrice" +
        "  from pricevolume natural join company" +
        "  where Industry = ?" +
        "    and TransDate >= ? and TransDate <= ?" +
        "  order by TransDate, Ticker";

	static final String getAllIndustries = 
            "select distinct Industry" +
            "  from company" +
            "  order by Industry";		
        
    static final String dropPerformanceTable =
        "drop table if exists Performance;";

    static final String createPerformanceTable =
        "create table Performance (" +
        "  Industry char(30)," +
        "  Ticker char(6)," +
        "  StartDate char(10)," +
        "  EndDate char(10)," +
        "  TickerReturn char(12)," +
        "  IndustryReturn char(12)" +
        "  );";
    
    static final String insertPerformance =
        "insert into Performance(Industry, Ticker, StartDate, EndDate, TickerReturn, IndustryReturn)" +
        "  values(?, ?, ?, ?, ?, ?);";

    
    static class IndustryData {
		// To Do: Create this class which  contains the list of the tickers, the common days, start date, and end date		
    }

    public static void main(String[] args) throws Exception {
    
        // Get connection properties
        Properties readerProps = new Properties();
        readerProps.load(new FileInputStream(defaultReaderParams));
        Properties writerProps = new Properties();
        writerProps.load(new FileInputStream(defaultWriterParams));

        try {
            // Setup Reader and Writer Connection
            setupReader(readerProps);
            setupWriter(writerProps);
            
            List<String> industries = getIndustries();
            System.out.printf("%d industries found%n", industries.size());
            for (String industry : industries) {
                System.out.printf("%s%n", industry);
            }
            System.out.println();            
            
			for (String industry : industries) {
                System.out.printf("Processing %s%n", industry);
                IndustryData iData = processIndustry(industry);
                if (iData != null && iData.tickerList.size() > 2) {
                    System.out.printf("%d accepted tickers for %s(%s - %s), %d common dates%n",
                        iData.tickerList.size(), industry, iData.startDate, iData.endDate, iData.commonDays);
                    processIndustryGains(industry, iData);
                }
                else
                    System.out.printf("Insufficient data for %s => no analysis%n", industry);
                System.out.println();
            }
            
            // Close everything you don't need any more 
			
            System.out.println("Database connections closed");
        } catch (SQLException ex) {
            System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n",
                                    ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
        }
    }
    
    static void setupReader(Properties connectProps) throws SQLException {
        String dburl = connectProps.getProperty("dburl");
        String username = connectProps.getProperty("user");
        readerConn = DriverManager.getConnection(dburl, connectProps);
        System.out.printf("Reader connection %s %s established.%n", dburl, username);

        getDates = readerConn.prepareStatement(getDatesQuery);
        getTickerDates = readerConn.prepareStatement(getTickerDatesQuery);
        getIndustryPriceData = readerConn.prepareStatement(getIndustryPriceDataQuery);
    } 
    
    
    static void setupWriter(Properties connectProps) throws SQLException {
        String dburl = connectProps.getProperty("dburl");
        String username = connectProps.getProperty("user");
        writerConn = DriverManager.getConnection(dburl, connectProps);
        System.out.printf("Writer connection %s %s established.%n", dburl, username);

        // Create Performance Table; If this table already exists, drop it first
        Statement tstmt = writerConn.createStatement();
        tstmt.execute(dropPerformanceTable);
        tstmt.execute(createPerformanceTable);
        tstmt.close();
        
        insertPerformanceData = writerConn.prepareStatement(insertPerformance);
    } 
    
    static List<String> getIndustries() throws SQLException {
		List<String> result = new ArrayList<>();
		// To Do: Execute the appropriate query (you need one of them) and return a list of the industries 
		return result;			
    }
    
    static IndustryData processIndustry(String industry) throws SQLException {
		// To Do: Execute the appropriate SQL queries (you need two of them) and return an object of IndustryData        
        return new IndustryData(tickers, numDays, startDate, endDate);
    }
    
    static void processIndustryGains(String industry, IndustryData data) throws SQLException {
		// To Do: 
		// In this method, you should calculate the ticker return and industry return. Look at the assignment description to know how to do that 
		// Don't forget to do the split adjustment
		// After those calculations, insert the data into the Performance table you created earlier. You may use the following way to do that for each company (or ticker) of an indsutry: 
			// insertPerformanceData.setString(1, industry);
			// insertPerformanceData.setString(2, ticker);
			// insertPerformanceData.setString(3, startdate);
			// insertPerformanceData.setString(4, enddate);
			// insertPerformanceData.setString(5, String.format("%10.7f", tickerReturn);
			// insertPerformanceData.setString(6, String.format("%10.7f", industryReturn);
			// int result = insertPerformanceData.executeUpdate();
	}
}
