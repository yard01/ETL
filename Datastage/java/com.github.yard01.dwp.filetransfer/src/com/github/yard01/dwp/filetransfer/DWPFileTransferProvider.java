package com.github.yard01.dwp.filetransfer;

import java.io.File;
import java.util.Properties;

public class DWPFileTransferProvider {

	protected static File stopFile;

	public static final long       VIEW_PAUSE = 10000; // 10 seconds 

	
	public static final int    DEFAULT_TIMEOUT = 60;	
	public static final String FTP_PROTOCOL  = "FTP";
	public static final String SFTP_PROTOCOL = "SFTP";
	public static final String FILE_PROTOCOL = "FILE";

	public static final String DB_DRIVER_PROPERTY 		= "db_driver";
	public static final String DB_CONNECTION_PROPERTY	= "db_connection";
	public static final String DB_USER_PROPERTY    		= "db_user";
	public static final String DB_PASSWORD_PROPERTY 	= "db_password";	
	
	public static final String PROTOCOL_PROPERTY 		= "protocol";	
	public static final String LOGIN_PROPERTY    		= "login";
	public static final String PASSWORD_PROPERTY 		= "password";
	public static final String HOST_PROPERTY     		= "host";
	public static final String PATH_PROPERTY     		= "path";
	public static final String DESTINATION_PROPERTY 	= "destination";
	public static final String TIMEOUT_PROPERTY			= "timeout";
	public static final String STOP_FLAG_PROPERTY		= "stopflag";
	public static final String ARCHIVE_TABLE_PROPERTY	= "table";
	

	//public static final String FILE_PROTOCOL = "FILE";
	
	
	public static IDWPStagingAreaFileTransfer createTransfer(Properties config) {
		
		String protocol = config.getProperty(PROTOCOL_PROPERTY);
		if (protocol.equals( FTP_PROTOCOL))  return new FTPTransfer();
		if (protocol.equals(SFTP_PROTOCOL))  return new SFTPTransfer();
		if (protocol.equals(FILE_PROTOCOL))  return new FileTransfer();
		
		//if 
		return null;
	}
	
	public static void pause() {
		try {
			Thread.sleep(VIEW_PAUSE);			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
}
