package com.cwc.threading;

import java.io.*;
import java.sql.*;
import java.util.Arrays;
import java.util.concurrent.RunnableFuture;

public class Thread1 extends Thread {

    PreparedStatement preparedStatement;
    int batchSize = 20;
    String filePath = "F:\\MultiThreading\\src\\com\\cwc\\threading\\vendor.csv";

    @Override
    public void run() {
        System.out.println("Thread 1 Started.........");
        Connection connection = ConnectionManager.manageConnection();
            try{
                //insert data
                insertInformation(connection);
                Thread.sleep(5000);
                //Get data from database
            getDataFromDatabase(connection);

            }catch (InterruptedException ex){
                System.err.println(ex);
            }
    }

    private void getDataFromDatabase(Connection connection) {
        try {
            //Select Query
            String select_sql = "select * from vendors_id";
            preparedStatement = connection.prepareStatement(select_sql);
            //select data from database
            boolean flag = false;
            ResultSet rs = preparedStatement.executeQuery();
            System.out.println("......Information From Database...........");
            while (rs.next()){
                flag = true;
                int sno = rs.getInt("sno");
                String vendorName = rs.getString("vendor_name");
                int tps = rs.getInt("tps");
                //System.out.println(rs.getInt(1) + "\t" + rs.getString(2) + "\t" + rs.getInt(3));
                System.out.println(sno + " \t " + vendorName + " \t " + tps);
            }
            if (flag != false) {
                connection.commit();
                connection.close();
            } else {
                System.out.println("No Record found...");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void insertInformation(Connection connection) {
        //Now Insert Data
        try {
            String lineText = null;
            int count = 0;
           String sql =  "INSERT into vendors_id (sno, vendor_name, tps) VALUES(?, ?, ?)";
            preparedStatement = connection.prepareStatement(sql);

            //insert data from file
            BufferedReader lineReader = new BufferedReader(new FileReader(filePath));
            lineReader.readLine();//Skip Header Line
            while ((lineText = lineReader.readLine())!= null){
                String[] data = lineText.split(",");
                String sno = data[0];
                String vendor_name = data[1];
                String tps = data[2];
                Integer s1 = Integer.parseInt(sno);
                preparedStatement.setInt(1,s1);
                preparedStatement.setString(2,vendor_name);
                Integer s2 = Integer.parseInt(tps);
                preparedStatement.setInt(3,s2);

                preparedStatement.addBatch();
                if(count % batchSize == 0){
                    int[] batch = preparedStatement.executeBatch();
                    System.out.println("Number of row affected " + Arrays.stream(batch).sum());
                }

            }
            lineReader.close();
            // execute the remaining queries
            preparedStatement.executeBatch();
//            connection.commit();
//            connection.close();
        } catch (IOException ex) {
            System.err.println(ex);
        } catch (SQLException ex) {
            ex.printStackTrace();

            try {
                connection.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
