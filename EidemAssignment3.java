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

class EidemAssignment3 {

    static final String defaultReaderParams = "readerparams.txt";
    static final String defaultWriterParams = "writerparams.txt";
    static Connection readerConn = null;
    static Connection writerConn = null;
    // To Do: Other variables

    static final String getDatesQuery = "select max(startDate), min(endDate)"
            + "  from (select Ticker, min(TransDate) as StartDate, max(TransDate) as endDate,"
            + "            count(distinct TransDate) as tradingDays" + "          from company natural join pricevolume"
            + "          where Industry = ?" + "          group by Ticker"
            + "          having tradingDays >= ?) as TickerDates";

    static final String getTickerDatesQuery = "select Ticker, min(TransDate) as StartDate, max(TransDate) as endDate,"
            + "      count(distinct TransDate) as tradingDays" + "  from company natural join pricevolume"
            + "  where Industry = ?" + "    and TransDate >= ? and TransDate <= ?" + "  group by Ticker"
            + "  having tradingDays >= ?" + "  order by Ticker";

    static final String getIndustryPriceDataQuery = "select Ticker, TransDate, OpenPrice, ClosePrice"
            + "  from pricevolume natural join company" + "  where Industry = ?" + // make it a var of current industry
            "    and TransDate >= ? and TransDate <= ?" + "  order by TransDate, Ticker";

    static final String getAllIndustries = "select distinct Industry" + "  from company" + "  order by Industry";

    static final String dropPerformanceTable = "drop table if exists Performance;";

    static final String createPerformanceTable = "create table Performance (" + "  Industry char(30),"
            + "  Ticker char(6)," + "  StartDate char(10)," + "  EndDate char(10)," + "  TickerReturn char(12),"
            + "  IndustryReturn char(12)" + "  );";

    static final String insertPerformance = "insert into Performance(Industry, Ticker, StartDate, EndDate, TickerReturn, IndustryReturn)"
            + "  values(?, ?, ?, ?, ?, ?);";

    static class IndustryData {
        // To Do: Create this class which contains the list of the tickers, the common
        // days, start date, and end date

        List<String> tickerList;
        String startDate, endDate;
        int commonDays;

        IndustryData(List<String> tickerList, String startDay, String endDay, int commonDays) {
            this.tickerList = tickerList;
            this.startDate = startDay;
            this.endDate = endDay;
            this.commonDays = commonDays;
        }

        public List<String> getTicker() {
            return tickerList;
        }

        public String getStartDate() {
            return startDate;
        }

        public String getEndDate() {
            return endDate;
        }
    }

    public static void main(String[] args) throws Exception {

        // Get connection properties
        Properties readerProps = new Properties();
        readerProps.load(new FileInputStream(defaultReaderParams));
        // Properties writerProps = new Properties();
        // writerProps.load(new FileInputStream(defaultWriterParams));

        try {
            // Setup Reader and Writer Connection
            setupReader(readerProps);
            // setupWriter(writerProps);

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
                    System.out.printf("%d accepted tickers for %s(%s - %s), %d common dates%n", iData.tickerList.size(),
                            industry, iData.startDate, iData.endDate, iData.commonDays);
                    // processIndustryGains(industry, iData);
                } else {
                    System.out.printf("Insufficient data for %s => no analysis%n", industry);
                }
                System.out.println();
            }

            // Close everything you don't need any more

            System.out.println("Database connections closed");
        } catch (SQLException ex) {
            System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n", ex.getMessage(), ex.getSQLState(),
                    ex.getErrorCode());
        }
    }

    static void setupReader(Properties connectProps) throws SQLException {
        String dburl = connectProps.getProperty("dburl");
        String username = connectProps.getProperty("user");
        readerConn = DriverManager.getConnection(dburl, connectProps);
        System.out.printf("Reader connection %s %s established.%n", dburl, username);

        // getDates = readerConn.prepareStatement(getDatesQuery);
        // getTickerDates = readerConn.prepareStatement(getTickerDatesQuery);
        // getIndustryPriceData =
        // readerConn.prepareStatement(getIndustryPriceDataQuery);
    }

    // static void setupWriter(Properties connectProps) throws SQLException {
    // String dburl = connectProps.getProperty("dburl");
    // String username = connectProps.getProperty("user");
    // writerConn = DriverManager.getConnection(dburl, connectProps);
    // System.out.printf("Writer connection %s %s established.%n", dburl, username);

    // // Create Performance Table; If this table already exists, drop it first
    // Statement tstmt = writerConn.createStatement();
    // tstmt.execute(dropPerformanceTable);
    // tstmt.execute(createPerformanceTable);
    // tstmt.close();

    // insertPerformanceData = writerConn.prepareStatement(insertPerformance);
    // }

    static List<String> getIndustries() throws SQLException {
        List<String> result = new ArrayList<>();
        // To Do: Execute the appropriate query (you need one of them) and return a list
        // of the industries

        Statement stmt = readerConn.createStatement();
        ResultSet results = stmt.executeQuery("select distinct Industry from company");

        while (results.next()) {
            String industry = results.getString("Industry");
            result.add(industry);
        }

        return result;
    }

    // static IndustryData processIndustry(String industry) throws SQLException {
    // // To Do: Execute the appropriate SQL queries (you need two of them) and
    // return
    // // an object of IndustryData

    // int numDays = -1;
    // String startDate = "-1";
    // String endDate = "-1";
    // List<String> tickers = new ArrayList<>();

    // Statement stmt = readerConn.createStatement();
    // // ResultSet tickersResult = stmt.executeQuery("select Ticker from company
    // where Industry = '" + industry + "'");

    // // while (tickersResult.next()) {
    // // String ticker = tickersResult.getString("Ticker");
    // // //System.out.println(ticker);
    // // tickers.add(ticker);
    // // }

    // // TODO:

    // // Need to get the max start date and the min end date!
    // // Should I go though that with using SQL or would it be easiser to go about
    // it
    // // with java code

    // // all the common days will not have null for any company

    // // ResultSet startDateResult = stmt.executeQuery("select Transdate from
    // company
    // // where Industry = '" + industry + "'");

    // ResultSet startDateResult = stmt.executeQuery(
    // "select max(startDate), min(endDate)"
    // + " from (select Ticker, min(TransDate) as StartDate, max(TransDate) as
    // endDate,"
    // + " count(distinct TransDate) as tradingDays"
    // + " from company natural join pricevolume" + " where Industry = '" + industry
    // + "' "
    // + " group by Ticker" + " having tradingDays >= StartDate) as TickerDates");

    // // System.out.println();

    // while (startDateResult.next()) {
    // startDate = startDateResult.getString("max(startDate)");
    // endDate = startDateResult.getString("min(endDate)");
    // }

    // // this query will get the min and max data of the common days and return the
    // count
    // ResultSet getTickerDatesQuery = stmt.executeQuery("select Ticker,
    // min(TransDate) as StartDate, max(TransDate) as endDate,"
    // + " count(distinct TransDate) as tradingDays" + " from company natural join
    // pricevolume"
    // + " where Industry = '" + industry + "' " + " and TransDate >= '" + startDate
    // + "' and TransDate <= '" + endDate + "' " + " group by Ticker" + " having
    // tradingDays >= '"
    // + startDate + "' " + " order by Ticker");

    // // getTickerDatesQuery = "select Ticker, min(TransDate) as StartDate,
    // max(TransDate) as endDate,"
    // // + " count(distinct TransDate) as tradingDays" + " from company natural
    // join pricevolume"
    // // + " where Industry = ?" + " and TransDate >= ? and TransDate <= ?" + "
    // group by Ticker"
    // // + " having tradingDays >= ?" + " order by Ticker";

    // while (getTickerDatesQuery.next()) {
    // numDays = Integer.parseInt(getTickerDatesQuery.getString("tradingDays"));

    // // This was an attempt to fix how I am getting too many tickers
    // // The result was it was giving me just a less
    // // - could this be accounting for the null that the write up talks about

    // String ticker = getTickerDatesQuery.getString("Ticker");
    // System.out.println(ticker);
    // tickers.add(ticker);
    // }

    // return new IndustryData(tickers, startDate, endDate, numDays);
    // }

    static IndustryData processIndustry(String industry) throws SQLException {
        // To Do: Execute the appropriate SQL queries (you need two of them) and return
        // an object of IndustryData



        int minDataDays = 150; //per write-up
        String minDataDay = "150";


        Statement stmt = readerConn.createStatement();


        //this gets the min and max date that I need
        String getDatesQuery = "select max(startDate), min(endDate)"
                + "  from (select Ticker, min(TransDate) as StartDate, max(TransDate) as endDate,"
                + "            count(distinct TransDate) as tradingDays"
                + "          from company natural join pricevolume" + "          where Industry = ?"
                + "          group by Ticker" + "          having tradingDays >= ?) as TickerDates";

        PreparedStatement getDates = readerConn.prepareStatement(getDatesQuery);        





        //this uses the dates that were found 
        String getTickerDatesQuery = "select Ticker, min(TransDate) as StartDate, max(TransDate) as endDate,"
                + "      count(distinct TransDate) as tradingDays" + "  from company natural join pricevolume"
                + "  where Industry = ?" + "    and TransDate >= ? and TransDate <= ?" + "  group by Ticker"
                + "  having tradingDays >= ?" + "  order by Ticker";

        PreparedStatement getTickerDates = readerConn.prepareStatement(getTickerDatesQuery);
        

        getDates.setString(1, industry);
        getDates.setInt(2, minDataDays);
        ResultSet rs = getDates.executeQuery();

        String startDate;
        String endDate;

        if (rs.next()) {
            startDate = rs.getString(1);
            endDate = rs.getString(2);
        } else {
            return null;
        }

        rs.close();

        getTickerDates.setString(1, industry);
        getTickerDates.setString(4, minDataDay);
        getTickerDates.setString(2, startDate);
        getTickerDates.setString(3, endDate);
        rs = getTickerDates.executeQuery();

        List<String> tickers = new ArrayList<>();
        int numDays = Integer.MAX_VALUE;
        while (rs.next()) {
            String ticker = rs.getString(1);
            int tickerDays = rs.getInt(4);
            if (tickerDays < numDays) {
                numDays = tickerDays;
            }
        }
        rs.close();

        return new IndustryData(tickers, startDate, endDate, numDays);
    } // end processIndustry

    static void processIndustryGains(String industry, IndustryData data) throws SQLException {
        // To Do:
        // In this method, you should calculate the ticker return and industry return.
        // Look at the assignment description to know how to do that
        // Don't forget to do the split adjustment
        // After those calculations, insert the data into the Performance table you
        // created earlier. You may use the following way to do that for each company
        // (or ticker) of an indsutry:

        Statement stmt = readerConn.createStatement();

        String indus = industry;
        List<String> ticker = data.getTicker();
        String startDate = data.getStartDate();
        String endDate = data.getEndDate();

        for (int i = 0; i < ticker.size(); i++) {
            String currentTicker = ticker.get(i);

            // for current ticker
            ResultSet currentTickerQuery = stmt.executeQuery("select P.TransDate, P.openPrice, P.closePrice "
                    + "from pricevolume P " + "where Ticker = '" + currentTicker + "' and TransDate >= '" + startDate
                    + "' " + "and TransDate <= '" + endDate + "' ");

            // for rest of tickers

            ResultSet allTickerQuery = stmt.executeQuery(
                    "select P.TransDate, P.openPrice, P.closePrice " + "from pricevolume P natural join company "
                            + "where Industry = '" + industry + "' " + "and TransDate >= '" + startDate
                            + "' and TransDate <= '" + endDate + "' " + "order by TransDate, Ticker");

        }

        /*
         * Trading intervals A stock will be considered for comparison if it has at
         * least 150 days of data
         * 
         * 
         */

        // insertPerformanceData.setString(1, industry);
        // insertPerformanceData.setString(2, ticker);
        // insertPerformanceData.setString(3, startdate);
        // insertPerformanceData.setString(4, enddate);
        // insertPerformanceData.setString(5, String.format("%10.7f", tickerReturn);
        // insertPerformanceData.setString(6, String.format("%10.7f", industryReturn);
        // int result = insertPerformanceData.executeUpdate();
    }
}
