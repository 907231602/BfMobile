package com.bf.mobile.data.flow.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.bf.mobile.data.flow.connetion.JdbcManager;

public class LogUvDao {
	public void addUv(String date,int countIp){
		Connection con=JdbcManager.getConnection();
		
		try {
			PreparedStatement ps= con.prepareStatement("insert into log_uv_ip values(?,?)"
		+ "ON DUPLICATE KEY UPDATE countip=?");
		ps.setString(1, date);
		ps.setString(2, Integer.toString(countIp));
		ps.setString(3, Integer.toString(countIp));

		ps.executeUpdate();
		ps.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
			
		}
}
