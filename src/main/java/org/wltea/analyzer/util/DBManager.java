package org.wltea.analyzer.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.druid.pool.DruidDataSource;

public class DBManager {
	
	private DruidDataSource ds;
	
	private static DBManager _instance = null;
	
	
	private DBManager(String drvier, String url, String user, String password) throws SQLException{		
		ds = new DruidDataSource();
		ds.setDriverClassName(drvier);
		ds.setUrl(url);
		ds.setUsername(user);
		ds.setPassword(password);
		ds.init();
	}
	
	public static synchronized DBManager getInstance(String drvier, String url, String user, String password) throws SQLException{
		if ( _instance == null ){
			_instance = new DBManager(drvier, url, user, password);
		}
		
		return _instance;
	}
	
	
	private Connection getConnection() throws SQLException{
		return ds.getConnection();
	}
	
	public static interface RSParser<T> {
		T parse(ResultSet rs);
	}
	
	
	public <T> List<T> query(String sql, RSParser<T> parser) throws SQLException{
		Connection conn = getConnection();
		ResultSet rs = conn.createStatement().executeQuery(sql);
		List<T> trs = new ArrayList<>();
		while ( rs.next() ){
			trs.add(parser.parse(rs));
		}
		conn.close();
		return trs;
	}
	
}
