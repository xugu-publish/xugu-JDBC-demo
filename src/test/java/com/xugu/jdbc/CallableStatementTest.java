package com.xugu.jdbc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.junit.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CallableStatementTest {

	static Connection con;
	static Statement stm;
	@BeforeClass
	public static void getConnecion()
	{
		try {
			con = GetConnection.getInstance().getConnection();
			stm = con.createStatement();
			testDDLSql(0);
			initialTable(1000);
			initialProcedure(0);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@AfterClass
	public static void releaseConnection()
	{
		try {
			initialProcedure(1);
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
	
	private static void initialTable(int count)
	{
		for(int i=1;i<=count;i++)
		{
			try {
				stm.execute("insert into testTable values(default,'xugu"+i+"','123456')");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private static void initialProcedure(int mode)
	{
		String pro1 = "create or replace procedure pro1(inId int,inName char(20),inPwd varchar)"
			+" as"
			+" begin"
			+" insert into testTable values(inId,inName,inPwd);"
			+" end;";
		String pro2 = "create or replace procedure pro2(returnCur out sys_refcursor,inId1 int,inId2 int)"
			+" as"
			+" begin"
			+" open returnCur for select * from testTable where id between inId1 and inId2;"
			+" end";
		String fun1 = "create or replace function fun1(inId int)"
			+" return varchar"
			+" as"
			+" returnStr varchar;"
			+" begin"
			+" select name||password into returnStr from testTable where id =inId;"
			+" return returnStr;"
			+" end;";
		String dpro1 = "drop procedure pro1";
		String dpro2 = "drop procedure pro2";
		String dfun1 = "drop procedure fun1";

		try {
			if(mode==0){
				
				stm.execute(pro1);
				stm.execute(pro2);
				stm.execute(fun1);
			}else{
				stm.execute(dpro1);
				stm.execute(dpro2);
				stm.execute(dfun1);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//@Test
	public void paramProcedureTest()
	{
		CallableStatement cstm = null;
		try {
			cstm = con.prepareCall("begin pro1(?,?,?);end");
			cstm.setInt(1, 2000);
			cstm.setString(2, "xugu 2000");
			cstm.setString(3, "123456");
			cstm.execute();
			String sql ="select * from testTable where id=2000";
			ResultSet rs = stm.executeQuery(sql);
			int returnId = 0;
			while(rs.next())
			{
				returnId = rs.getInt(1); 
			}
			Assert.assertEquals(2000,returnId);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				if(cstm != null){
					cstm.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	//@Test
	public void refCursorProcedureTest()
	{
		CallableStatement cstm = null;
		try {
			cstm = con.prepareCall("{call pro2(?,?,?)}");
			cstm.registerOutParameter(1, com.xugu.cloudjdbc.Types.REFCUR);
			cstm.setInt(2, 20);
			cstm.setInt(3, 100);
			cstm.execute();
			
			int totalRows = 0;
			ResultSet refRs = (ResultSet)cstm.getObject(1);
			while(refRs.next())
			{
				totalRows++;
			}
			org.junit.Assert.assertEquals(81, totalRows);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				if(cstm != null){
					cstm.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	//@Test
	public void functionTest()
	{
		CallableStatement cstm = null;
		try {
			cstm = con.prepareCall("{?=call fun1(?)}");
			cstm.registerOutParameter(1, Types.VARCHAR);
			cstm.setInt(2, 20);
			cstm.execute();
			
			String returnStr = cstm.getString(1);
			org.junit.Assert.assertEquals("xugu20123456", returnStr);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				if(cstm != null){
					cstm.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	@Test
	public void setParamByNameTest()
	{
		CallableStatement cstm = null;
		ResultSet rs = null;
		try {
			cstm = con.prepareCall("update testTable set password=:pwd where id=:id");
			cstm.setString("pwd", "654321");
			cstm.setInt("id", 200);
			int updateCount = cstm.executeUpdate();
			if(updateCount==1)
			{
				rs = stm.executeQuery("select * from testTable where id=200");
				while(rs.next())
				{
					System.out.println(rs.getInt(1)+" -- "+rs.getString(2)+" -- "+rs.getString(3));
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				if(rs != null){
					rs.close();
				}
				if(cstm != null){
					cstm.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
