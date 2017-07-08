package com.bf.mobile.data.flow.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.bf.mobile.data.flow.connetion.JdbcManager;

public class PVFlowDao {
		public void addFlow(String sqlTableName,String dates,String phone,String count){
			Connection con= JdbcManager.getConnection();
			
			try {
			PreparedStatement ps=	con.prepareStatement("insert into pv_flow values(?,?,?) "
					+"ON DUPLICATE KEY UPDATE pvcount=?");
			ps.setString(1, dates);
			ps.setString(2, phone);
			ps.setString(3, count);
			ps.setString(4, count);
			
			//ps.setString(5, count);
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
