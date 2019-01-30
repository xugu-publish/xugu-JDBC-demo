package com.xugu.jdbc;

import java.sql.*;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.sql.PooledConnection;

import com.xugu.pool.XgConnectionPoolDataSource;
import com.xugu.pool.XgPooledConnection;

public class GetConnection {
	private Connection conn = null;
	private static GetConnection getConn;
	private GetConnection(){
	}
	
	public Connection getConnection(){
		 try {
			 DriverManager.registerDriver(new com.xugu.cloudjdbc.Driver());
			 Properties conPro = new Properties();
			 conPro.setProperty("user","SYSDBA");
			 conPro.setProperty("password","SYSDBA");
			 conPro.setProperty("return_rowid","true");
			 conPro.setProperty("char_set","utf8");
			 conPro.setProperty("lob_ret","descriptor");
			 conPro.setProperty("emptystringasNull","SYSDBA");
			 conn = DriverManager.getConnection("jdbc:xugu://192.168.2.212:5140/system", conPro);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}
	
	//xugu连接池设置
	public PooledConnection getPConnection()
	{
		XgConnectionPoolDataSource xgCPDSource = null;
		XgPooledConnection xgPconn = null;
		try {
			xgCPDSource = new XgConnectionPoolDataSource();
			xgCPDSource.setHostName("192.168.2.215");
			xgCPDSource.setPort(Integer.parseInt("1234"));
			xgCPDSource.setDatabaseName("gt");
			xgCPDSource.setUser("gtcw0122");
			xgCPDSource.setPassword("123");
			xgCPDSource.setUrl("jdbc:xugu://192.168.2.215:1234/gt");
			xgCPDSource.setMaxActive(5);
			xgCPDSource.setMinIdle(2);
			xgCPDSource.setLoginTimeout(100);
			xgPconn = (XgPooledConnection)xgCPDSource.getPooledConnection();
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return xgPconn;
	}
	
	
	public Connection getMoreIPsConnection()
	{
		try {
			Class.forName("com.xugu.cloudjdbc.Driver");
			conn = DriverManager.getConnection("jdbc:xugu://192.168.2.214:5139/System?user=SYSDBA&password=SYSDBA&ips=192.168.2.216,192.168.2.215,192.168.2.217");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return conn;
	}
	
	
	public Connection getConnectionByProps() throws ClassNotFoundException, SQLException
	{
		Class.forName("com.xugu.cloudjdbc.Driver");
		Properties info=new Properties();
		Vector<String> ipsVector=new Vector<String>();
		ipsVector.add("192.168.1.201");
		ipsVector.add("192.168.1.204");
		ipsVector.add("192.168.1.205");
		ipsVector.add("192.168.1.206");
		
		info.put("ips", ipsVector);
//		String[] ips={"192.168.1.201","192.168.1.204","192.168.1.206"};
//		info.put("ips", ips);
		
		conn = DriverManager.getConnection("jdbc:xugu://192.168.1.201:5138/SYSTEM?user=SYSDBA&password=SYSDBA",info);
		return conn;
	}
	public static GetConnection getInstance(){
		if(getConn == null)
		{
			getConn = new GetConnection();
		}
		return getConn;
			
		
	}
	
	
}
