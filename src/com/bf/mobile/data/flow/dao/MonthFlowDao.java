package com.bf.mobile.data.flow.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.bf.mobile.data.flow.connetion.JdbcManager;

public class MonthFlowDao {
	public void addFlow(String phoneMonth,String phoneNumber,String upFlow,String downFlow,String totalFlow){
		Connection con=JdbcManager.getConnection();
		
		try {
			PreparedStatement ps= con.prepareStatement("insert into month_phone values(?,?,?,?,?)"
		+ "ON DUPLICATE KEY UPDATE phoneupflow=?,phonedownflow=?,phonetotalflow=?");
		ps.setString(1, phoneMonth);
		ps.setString(2, phoneNumber);
		ps.setString(3, upFlow);
		ps.setString(4, downFlow);
		ps.setString(5, totalFlow);
		ps.setString(6, upFlow);
		ps.setString(7, downFlow);
		ps.setString(8, totalFlow);
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
