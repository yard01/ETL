package com.github.yard01.dwp.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.github.yard01.dwp.filetransfer.DWPFileTransferProvider;

public class DWPConnector {

	private static Connection connection = null;
	
	public static Connection getConnection(Properties config) throws ClassNotFoundException, SQLException {
		
		String driverString = config.getProperty(DWPFileTransferProvider.DB_DRIVER_PROPERTY);
		
		if (driverString == null) return null;
		
		if (connection != null) return connection;

		Class.forName(driverString);
		
		connection = DriverManager.getConnection(
							config.getProperty(DWPFileTransferProvider.DB_CONNECTION_PROPERTY),
							config.getProperty(DWPFileTransferProvider.DB_USER_PROPERTY),
							config.getProperty(DWPFileTransferProvider.DB_PASSWORD_PROPERTY)			
						);			
		
		return connection;
		
		
	}
}
