package com.cwc.threading;

import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class ConnectionManager {
    public static Connection con;
   public static Connection manageConnection(){
       try {
           MysqlConnectionPoolDataSource ds = new MysqlConnectionPoolDataSource();
           ds.setUrl("jdbc:mysql://localhost:3306/testdb");
           ds.setUser("root");
           ds.setPassword("root");
            con = ds.getConnection();
           con.setAutoCommit(false);
           System.out.println("Connection Established successfully....");//
       }catch (SQLException  e){
           e.printStackTrace();
       }
       return con;
   }
}
