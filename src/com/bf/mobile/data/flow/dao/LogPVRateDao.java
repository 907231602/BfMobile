package com.bf.mobile.data.flow.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.bf.mobile.data.flow.connetion.JdbcManager;

public class LogPVRateDao {
	

	public void addRation(String iterable_element,float jump,float pv,float ration){
		Connection con=	JdbcManager.getConnection();
		try {
		PreparedStatement ps=	con.prepareStatement("insert into log_pv_ration values(?,?,?,?)"+
		 "ON DUPLICATE KEY UPDATE jump=?,pv=?,ration=?");
			ps.setString(1,iterable_element);
			ps.setString(2, Float.toString(jump));
			ps.setString(3,  Float.toString(pv));
			ps.setString(4,  Float.toString(ration));
			
			ps.setString(5, Float.toString(jump));
			ps.setString(6,  Float.toString(pv));
			ps.setString(7,  Float.toString(ration));
			
			ps.executeUpdate();
			
			ps.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		finally{
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
