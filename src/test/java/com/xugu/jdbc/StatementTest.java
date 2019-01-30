package com.xugu.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class StatementTest {
	static Connection con;
	static Statement stm;
	@BeforeClass
	public static void getConnecion()
	{
		try {
			con = GetConnection.getInstance().getConnection();
			stm = con.createStatement();
			testDDLSql(0);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@AfterClass
	public static void releaseConnection()
	{
		try {
			testDDLSql(1);
			if(con != null)
			{
				con.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	public static void testDDLSql(int mode)
	{
		String createSql ="create table testTable(id int identity(1,1),name char(20),password varchar(50))";
		String trunSql = "truncate table testTable";
		String dropSql = "drop table testTable";
		try {
			if(mode==0){
				
				stm.execute(createSql);
			}else{
				
				stm.execute(dropSql);
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	@Test
	public void testExecuteUpdateSql()
	{
		String insertSql = "insert into testTable(name,password) values('xugu','123456')";
		String updateSql = "update testTable set password='654321' where name='xugu'";
		String deleteSql = "delete from testTable where id=1";
		try {
			int count = stm.executeUpdate(insertSql);
			Assert.assertEquals(1, count);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	//@Test
	public void testSelectSql()
	{
		String selectSql = "select * from testTable where id=1";
		ResultSet rs = null;
		try {
			rs = stm.executeQuery(selectSql);
			ResultSetMetaData rsmd = rs.getMetaData();
			int colCount = rsmd.getColumnCount();
			while(rs.next())
			{
				for(int i=1;i<=colCount;i++){
					
					System.out.println(rs.getObject(i));
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				if(rs !=null){
					
					rs.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	//@Test
	public void testMoreResultSet()
	{
		ResultSet rs = null;
		int updateCount = 0;
		String moreSelectSql ="select name from testTable where id=1;select password from testTable where id=1";
		try {
			boolean f = stm.execute(moreSelectSql);
			if(f == true){
				
				rs = stm.getResultSet();
			}else{
				
				updateCount = stm.getUpdateCount();
			}
			if(rs != null){

				while(rs.next())
				{
				   System.out.println("rs1: "+rs.getObject(1));
				}
				System.out.println("-----------------");
				
			}else{
				System.out.println("影响条数1: "+updateCount);
			}
			int num = 2;
			while(true)
			{
				if(!stm.getMoreResults(Statement.CLOSE_CURRENT_RESULT) && stm.getUpdateCount() == -1)
					break;
				else
				{
					
					if((rs = stm.getResultSet())!=null)
					{
						while(rs.next())
						{
							System.out.println("rs"+num+": "+rs.getObject(1));
						}
						System.out.println("-----------------");
						
					}else
					{
						System.out.println("影响条数"+num+": "+stm.getUpdateCount());
						
					}
					
				}
				
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				if(rs !=null){
					
					rs.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	@Test
	public void testGetGeneratedKey()
	{
		ResultSet rs = null;
		String insertSql = "insert into testTable values(default,'ouguan','123')";
		String updateSql = "update testTable set password='321' where id=";
		try {
			stm.executeUpdate(insertSql, Statement.RETURN_GENERATED_KEYS);
			rs = stm.getGeneratedKeys();
			while(rs.next())
			{
				
				updateSql+=rs.getString(1);
				int count = stm.executeUpdate(updateSql);
				System.out.println("update count: "+count);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				if(rs !=null){
					
					rs.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
