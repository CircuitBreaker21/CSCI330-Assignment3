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

//import jdk.javadoc.internal.doclets.toolkit.resources.doclets;

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
        String industry;
        String startDate, endDate;
        int commonDays;


        ArrayList<TickerList> stocksList; 

  

        IndustryData(List<String> tickerList, String startDay, String endDay, int commonDays) {
            this.tickerList = tickerList;
            this.startDate = startDay;
            this.endDate = endDay;
            this.commonDays = commonDays;
        }

        
        IndustryData(String Industry, ArrayList<TickerList> Stocks){
            industry = Industry;
            stocksList = Stocks;
        }

        public ArrayList<TickerList> getTickersList(){
            return stocksList;
        }
        
        public void acountSplits(){
            for (int i = 0; i < stocksList.size(); i++) {
                ArrayList<StockData> stocks = stocksList.get(i).getTickerList();


                double scaleFactor = 1;
                double prevOpenPrice = -1;
                int splitCount = 0;
                int tradingDays = 0;
                int scaleCount1 = 0; // 2:1
                int scaleCount2 = 0; // 3:1
                int scaleCount3 = 0; // 3:2


                ArrayList<StockData> result = new ArrayList<StockData>();
                for (int j = 0; j < stocks.size(); j++) {
                    StockData stock = stocks.get(j);
                    
                    
                    //process data for individual ticker
                    String ticker = stock.getTicker();
                    String transDate = stock.getDate();
                    double openPrice = stock.getOpenPrice();
                    double closePrice = stock.closePrice;
                    
                    
                    if (tradingDays == 0) {
                        prevOpenPrice = openPrice;
                        System.out.println(stock.getTicker());

                    }

                    double todaysRatio = closePrice / prevOpenPrice;

                    double adjustedPrice = -1;

                    if (Math.abs(todaysRatio - 2.0) < 0.2) {
                        scaleCount1++; // 2:1 split
                        splitCount++;
                        adjustedPrice = closePrice / 2;
                        System.out.println("2:1 split on " + transDate + "\t" + closePrice + "-->" + adjustedPrice);

                    } else if (Math.abs(todaysRatio - 3.0) < 0.3) { // 3:1 split
                        scaleCount2++;
                        splitCount++;
                        adjustedPrice = closePrice / 3;
                        System.out.println("3:1 split on " + transDate + "\t" + closePrice + "-->" + adjustedPrice);

                    } else if (Math.abs(todaysRatio - 1.5) < 0.15) { // 3:2 split
                        scaleCount3++;
                        splitCount++;
                        adjustedPrice = closePrice / 1.5;

                        System.out.println("3:2 split on " + transDate + "\t" + closePrice + "-->" + adjustedPrice);
                    }

                    // This will set the numbers going to the deQue to the com compounding ratio
                    scaleFactor = ((1 / (Math.pow(2, scaleCount1))) * (1 / (Math.pow(3, scaleCount2)))
                            * (1 / (Math.pow(1.5, scaleCount3))));
                    openPrice *= scaleFactor;
                    closePrice *= scaleFactor;

                    // Insert to the result
                    StockData s = new StockData(ticker, transDate, openPrice, closePrice);
                    result.add(s);

                    // Next Trading Day
                    prevOpenPrice = openPrice;
                    tradingDays++;


                }

                //Replace the return stock array with the one stored in stock list
                stocksList.get(i).replaceList(result);

                //System.out.println(splitCount + " splits in " + tradingDays + " trading days");
            }

        }

        public void printIndustryData(){
            for(int i = 0; i < stocksList.size(); i++){
                ArrayList<StockData> stocks = stocksList.get(i).getTickerList();
                for(int j = 0; j < stocks.size(); j++){
                    StockData stock = stocks.get(j);
                    System.out.println(industry + "\t" + stock.getTicker() + " \t" + stock.getDate() + "\t" + stock.getOpenPrice()
                            + " - " + stock.getClosePrice());
                    
                }
            }
        }
    }


    public static class TickerList{
        ArrayList<StockData> tickerList;

        TickerList(){
            tickerList = new ArrayList<StockData>();
        }

        public void addStockData(StockData stock){
            tickerList.add(stock);
        }

        public ArrayList<StockData> getTickerList(){
            return tickerList;
        }

        public void replaceList(ArrayList<StockData> result){
            tickerList = result;
        }
    }

    public static class StockData {
        String ticker;
        String date;
        double openPrice;
        double closePrice;

        StockData(String Ticker, String Date, double OpenPrice, double ClosePrice){
            ticker = Ticker;
            date = Date;
            openPrice = OpenPrice;
            closePrice = ClosePrice;
        }

        public String getTicker(){
            return ticker;
        }

        public String getDate(){
            return date;
        }

        public Double getOpenPrice(){
            return openPrice;
        }

        public Double getClosePrice(){
            return closePrice;
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
                    processIndustryGains(industry, iData);
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

 





    static IndustryData processIndustry(String industry) throws SQLException {
        // To Do: Execute the appropriate SQL queries (you need two of them) and return
        // an object of IndustryData

        int minDataDays = 150; // per write-up
        String minDataDay = "150";

        // this gets the min and max date that I need
        String getDatesQuery = "select max(startDate), min(endDate)"
                + "  from (select Ticker, min(TransDate) as StartDate, max(TransDate) as endDate,"
                + "            count(distinct TransDate) as tradingDays"
                + "          from company natural join pricevolume" + "          where Industry = ?"
                + "          group by Ticker" + "          having tradingDays >= ?) as TickerDates";

        PreparedStatement getDates = readerConn.prepareStatement(getDatesQuery);

        // this uses the dates that were found
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
            tickers.add(ticker);
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

        int intervalDays = 60;

        int numTickers = data.tickerList.size();
        Map<String, Integer> tickerIndex = new HashMap<>();
        {
            int index = 0;
            for (String ticker : data.tickerList) {
                tickerIndex.put(ticker, index);
                index++;
            }
        }
        int numIntervals = data.commonDays / intervalDays;

        String getIndustryPriceDataQuery = "select Ticker, TransDate, OpenPrice, ClosePrice"
                + "  from pricevolume natural join company" + "  where Industry = ?"
                + "    and TransDate >= ? and TransDate <= ?" + "  order by TransDate, Ticker";

        PreparedStatement getIndustryPriceData = readerConn.prepareStatement(getIndustryPriceDataQuery);

        getIndustryPriceData.setString(1, industry);
        getIndustryPriceData.setString(2, data.startDate);
        getIndustryPriceData.setString(3, data.endDate);

        ResultSet rs = getIndustryPriceData.executeQuery();
        
        
        
        String tickerQuery = "-1";
        String transDateQuery = "-1";
        String openPriceQuery = "-1";
        String closePriceQuery = "-1";
        
        ArrayList<StockData> stocks = new ArrayList<StockData>(); 





        Boolean firstDay = true;
        String firstDate = "";

        HashMap<String, Integer> tickerIndexHash = new HashMap<String, Integer>();

        ArrayList<String> tickers = new ArrayList<String>();
        ArrayList<TickerList> tickersList = new ArrayList<TickerList>();


        while(rs.next()){
            tickerQuery = rs.getString("Ticker");
            transDateQuery = rs.getString("TransDate");
            openPriceQuery = rs.getString("OpenPrice");
            closePriceQuery = rs.getString("ClosePrice");

            if(!tickerIndexHash.containsKey(tickerQuery)){

            }

            if(!tickers.contains(tickerQuery)){
                tickers.add(tickerQuery);

                tickersList.add(new TickerList());
                //tickersList.add(tickers);
            }

            
            //make a class of this data and make an arraylist to hold that class
            //consider the flow of dates
            //System.out.println(industry + "\t" + tickerQuery + " \t" + transDateQuery + "\t" + openPriceQuery + " - " + closePriceQuery);
            
            
            int tickerNumber = tickers.indexOf(tickerQuery);
            StockData stock = new StockData(tickerQuery, transDateQuery, Double.parseDouble(openPriceQuery), Double.parseDouble(closePriceQuery));
            
            tickersList.get(tickerNumber).addStockData(stock);



            //TODO:
            //Make an arrayList that holds 60 data points at a time 

            //make a class of industry interval
            //make a list of that class 
            //have that class have a return that is what I want to put into my database
        }

        IndustryData industryList = new IndustryData(industry, tickersList);

        

       


        //TODO
        //Must account for splits before continuing 


            //maybe make an arrraylost

            industryList.acountSplits();




        //TODO:
        //Ticker Return math




            ArrayList<TickerList> stocksList = industryList.getTickersList();

            for (int i = 0; i < stocksList.size(); i++) {
                ArrayList<StockData> stocksClass = stocksList.get(i).getTickerList();


                //need to go though the current ticker and get the date bounds for each interval
                //      also need to get the start and end prices for that interval 

                int intervalStart = 0;
                int intervalEnd = intervalStart + 60;

                String StartDate;
                String EndDate;

                Double TickerReturn;
                Double IndustryReturn;



                //Dont need forloops to go though the whole ticker set, just need the start and end ticker

                for (int j = intervalStart; j < intervalEnd; j++) {
                    StockData stock = stocksClass.get(j);
                    

                }

                //ned to go though the rest if the tickers and get the same data 
                for (int j = 0; j < stocksList.size(); j++) {
                    ArrayList<StockData> stocksClassExclusive = stocksList.get(j).getTickerList();
                    while(j != i){ //Filter out current stock with rest of stocks 


                        // Dont need forloops to go though the whole ticker set, just need the start and
                        // end ticker
                        for (int k = intervalStart; k < intervalEnd; k++) {
                            StockData stock = stocksClassExclusive.get(k);



    
                        }

                    }

                    // ned to go though the rest if the tickers and get the same data

                }
            }








        //TODO:
        //Industry return 









        // insertPerformanceData.setString(1, industry);
        // insertPerformanceData.setString(2, ticker);
        // insertPerformanceData.setString(3, startdate);
        // insertPerformanceData.setString(4, enddate);
        // insertPerformanceData.setString(5, String.format("%10.7f", tickerReturn);
        // insertPerformanceData.setString(6, String.format("%10.7f", industryReturn);
        // int result = insertPerformanceData.executeUpdate();
    }







}
