package com.bf.mobile.data.flow.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;



public class WebSiteDao {
	public void addFlow(String phoneDate,String phoneNumber,String webSite,int visitCount){
		Connection connection = com.bf.mobile.data.flow.connetion.JdbcManager.getConnection();
		try {
			PreparedStatement ps = connection.prepareStatement(
					"insert into website_phone_count values(?,?,?,?)"+"ON DUPLICATE KEY UPDATE phonecount = ?");
			ps.setString(1,phoneDate);
			ps.setString(2, phoneNumber);
			ps.setString(3, webSite);
			ps.setInt(4, visitCount);
			ps.setInt(5, visitCount);
			
			ps.executeUpdate();
			ps.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				connection.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
