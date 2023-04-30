package com.cwc.threading;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

public class Thread2 implements Runnable{
    private PreparedStatement preparedStatement;
    int batchSize = 20;
    String filePath = "F:\\MultiThreading\\src\\com\\cwc\\threading\\msg.csv";

    @Override
    public void run() {
        System.out.println("Thread 2 Started.....");
        try{
            //Connection Object
            Connection connection = ConnectionManager.manageConnection();
            //Read data from file and insert into database
            Thread.sleep(5000);
            insertFromCsvFile(connection);
            readDataFromMsgFile(connection);
        }catch(InterruptedException e){
            e.printStackTrace();
        }
    }

    private void readDataFromMsgFile(Connection connection) {
        try {
            //Select Query
            String select_sql = "select * from message_to_send";
            preparedStatement = connection.prepareStatement(select_sql);
            //select data from database
            boolean flag = false;
            ResultSet rs = preparedStatement.executeQuery();

            System.out.println("......Information From Database Thread 2 ...........");
            while (rs.next()){
                flag = true;
                int sno = rs.getInt("sno");
                String name = rs.getString("name");
                String message = rs.getString("message");
                String vendorName = rs.getString("vendor_name");
                String msgsent = rs.getString("msgsent");
                //System.out.println(rs.getInt(1) + "\t" + rs.getString(2) + "\t" + rs.getInt(3));
                System.out.println(sno + " \t " + name + " \t " + message + " \t " + vendorName + " \t " + msgsent);
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

    private void insertFromCsvFile(Connection connection) {
            //Get data from file and insert into database
            try {
                String lineText = null;
                int count = 0;
                String sql =  "INSERT into message_to_send (sno,name,message,vendor_name, msgsent) VALUES(?, ?, ?, ?, ?)";
                preparedStatement = connection.prepareStatement(sql);

                //insert data from file
                BufferedReader lineReader = new BufferedReader(new FileReader(filePath));
                lineReader.readLine();//Skip Header Line
                while ((lineText = lineReader.readLine())!= null){
                    String[] d1 = lineText.split(",");
                    String sno = d1[0];
                    String name = d1[1];
                    String message = d1[2];
                    String vendor_name = d1[3];
                    String msgsent = d1[4];
                    Integer s1 = Integer.parseInt(sno);
                    preparedStatement.setInt(1,s1);
                    preparedStatement.setString(2,name);
                    preparedStatement.setString(3,message);
                    preparedStatement.setString(4,vendor_name);
                    preparedStatement.setString(5,msgsent);
                    preparedStatement.addBatch();
                    if(count % batchSize == 0){
                        int[] batch = preparedStatement.executeBatch();
                        System.out.println("Number of row affected ");
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
