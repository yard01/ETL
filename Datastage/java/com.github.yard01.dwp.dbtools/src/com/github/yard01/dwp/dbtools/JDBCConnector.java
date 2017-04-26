package com.github.yard01.dwp.dbtools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * 
 * @author Dmitry Yaroslavtsev (YarD)
 * 
 *
 */

public class JDBCConnector {

	public static final String DB_DRIVER_PROPERTY     = "db_driver";
	public static final String DB_CONNECTION_PROPERTY = "db_connection";	
	public static final String DB_USER_PROPERTY       = "db_user";
	public static final String DB_PASSWORD_PROPERTY   = "db_password";
	
	
	private static Connection connection = null;
	
	public static Connection getConnection(Properties config) throws ClassNotFoundException, SQLException {
		
		String driverString = config.getProperty(DB_DRIVER_PROPERTY);
		
		if (driverString == null) return null;
		
		Class.forName(driverString);
		
		connection = DriverManager.getConnection(
							config.getProperty(DB_CONNECTION_PROPERTY),
							config.getProperty(DB_USER_PROPERTY),
							config.getProperty(DB_PASSWORD_PROPERTY)			
						);			
		
		return connection;
		
		
	}

}
