package com.xugu.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import com.xugu.cloudjdbc.Blob;
import com.xugu.cloudjdbc.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PreparedStatementTest {
	Connection con = null;
	@Before
	public void getConnection()
	{
		con = GetConnection.getInstance().getConnection();
		ddlSql(0);
	}
	
	@After
	public void releaseConnection()
	{
		try {
			ddlSql(1);
			if(con != null){
				con.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void ddlSql(int mode)
	{
		Statement stm = null;
		String createSql ="create table testTable(id int identity(1,1),name char(20),password varchar(50),marks clob)";
		String createLobSql="create table testLob(id int primary key,name char(20),photo blob,remark clob,other blob)";
		String dropSql = "drop table testTable";
		String dropLobSql ="drop table testLob";
		try {
			stm = con.createStatement();
			if(mode==0){
				stm.execute(createSql);
				stm.execute(createLobSql);
				
			}
			else{
				stm.execute(dropSql);
				stm.execute(dropLobSql);
				
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				if(stm != null){
					stm.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
	}
	
	//@Test
	public void batchInsertTest()
	{
		PreparedStatement pstm = null;
		try {
			pstm = con.prepareStatement("insert into testTable(name,password,marks) values(?,?,?)");
			for(int i=1;i<1000;i++)
			{
				pstm.setString(1, "xugu: "+i);
				pstm.setString(2, "123456");
				pstm.setString(3, "This is Clob column");
				pstm.addBatch();
				
			}
			
			int[] inserts = pstm.executeBatch();
			Assert.assertEquals(999, inserts.length);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				if(pstm != null){
					pstm.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	//@Test
	public void lobInsertTest()
	{
		PreparedStatement pstm = null;
		String path = System.getProperty("user.dir");
		try {
			pstm = con.prepareStatement("insert into testLob values(?,?,?,?,?)");
			FileInputStream inBlob = new FileInputStream(new File(path+"\\lobFile\\blob.jpg"));
			FileReader rdClob = new FileReader(new File(path+"\\lobFile\\clob.txt"));
			Blob xuguBlob = new Blob(inBlob);
			Clob xuguClob = new Clob(rdClob);
			Blob emptyBlob = Blob.createTemporary((com.xugu.cloudjdbc.Connection)con, false, 2);
			pstm.setInt(1, 1);
			pstm.setString(2, "xugu");
			pstm.setBlob(3, xuguBlob);
			pstm.setClob(4, xuguClob);
			pstm.setBlob(5, emptyBlob);
			int insertCount = pstm.executeUpdate();
			emptyBlob.freeTemporary();
			Assert.assertEquals(1, insertCount);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				pstm.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
	}
	
	//@Test
	public void updateTest()
	{
		PreparedStatement pstm = null;
		PreparedStatement pstmSelect = null;
		try {
			pstm = con.prepareStatement("update testLob set other=? where id=1");
			pstmSelect = con.prepareStatement("select id from testTable where id=?");
			pstmSelect.setInt(1, 1);
			ResultSet rs = pstmSelect.executeQuery();
			if(!rs.next()){
				lobInsertTest();
			}
			
			pstm.setNull(1, java.sql.Types.BLOB);
			int updateCount = pstm.executeUpdate();
			Assert.assertEquals(1, updateCount);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				pstmSelect.close();
				pstm.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	 
	@Test
	public void testGetGeneratedKey()
	{
		PreparedStatement pstm = null;
		try {
			pstm = con.prepareStatement("insert into testTable values(default,?,?,?)",new String[]{"ID"});
			pstm.setString(1, "xugu: 1001");
			pstm.setString(2, "654321");
			pstm.setObject(3, null);
			pstm.execute();
			ResultSet rs = pstm.getGeneratedKeys();
			while(rs.next())
			{
				System.out.println("insert id: "+rs.getInt(1));
				
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				pstm.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
	}
	

}
