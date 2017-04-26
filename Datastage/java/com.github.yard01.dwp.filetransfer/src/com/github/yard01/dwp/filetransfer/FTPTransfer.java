package com.github.yard01.dwp.filetransfer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import com.github.yard01.dwp.db.DWPConnector;
import com.github.yard01.dwp.filetransfer.archiver.DWPArchiveProvider;

import sun.net.ftp.FtpClient;
import sun.net.ftp.FtpProtocolException;

public class FTPTransfer extends FtpClient implements IDWPStagingAreaFileTransfer  {
		
	public static final int BUFFER_SIZE = 10240;
//	public static final String TMP_SUFFIX = ".tmp";
	
//	private Connection connection;
//	private String archiveTable = "";
	
	/*
	public static void main(String[] args)  {
		
		FTPTransfer ftp = new FTPTransfer();
		Properties cfg = new Properties();
		cfg.put("db_driver", "com.ibm.db2.jcc.DB2Driver");
		cfg.put("db_connection", "jdbc:db2://SSUMOD:50000/DBDWH");		
		cfg.put("db_user", "dsadm");
		cfg.put("db_password", "dsadm");
		cfg.put("table", "ARCHIVE.ARCHIVE_DWP");
		cfg.put("destination", "E:/Tools");
		cfg.put("stopflag", "E:/Tools/stop.flag");		
		cfg.put("timeout", "50");		
		cfg.put("login", "ssdtrans");
		cfg.put("password", "ssdtrans");
		cfg.put("path", "/ODD/DWP/*.pw");
		cfg.put("host", "paumod");			
	
		try {
			ftp.start(cfg);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}			
	}*/

		
	//@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

	//@Override
	public void start(Properties config) throws IOException, ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		
		String timeout           = config.getProperty(DWPFileTransferProvider.TIMEOUT_PROPERTY);
		String destinationFolder = config.getProperty(DWPFileTransferProvider.DESTINATION_PROPERTY);
		File stopFile            = new File(config.getProperty(DWPFileTransferProvider.STOP_FLAG_PROPERTY));		
		String archiveTable      = config.getProperty(DWPFileTransferProvider.ARCHIVE_TABLE_PROPERTY);
		int timeoutInt 			 = DWPFileTransferProvider.DEFAULT_TIMEOUT;
		
		try {			
			timeoutInt = Integer.valueOf(timeout);
		} catch (Exception e) { 
		}
		this.setConnectTimeout(timeoutInt);
		this.setReadTimeout(timeoutInt);
		
		openServer(config.getProperty(DWPFileTransferProvider.HOST_PROPERTY));				
		login(config.getProperty(DWPFileTransferProvider.LOGIN_PROPERTY), config.getProperty(DWPFileTransferProvider.PASSWORD_PROPERTY));
		
		
		Connection connection    = DWPConnector.getConnection(config);
		////////////////////////////////////////////////////////////////////////////////////////////
		//Read remote directory
		while (!stopFile.exists()) {
			DWPFileTransferProvider.pause();				
			this.ascii();
			String[] names = null;
			try {
				names = getFileList(config.getProperty(DWPFileTransferProvider.PATH_PROPERTY));
			} catch (SocketTimeoutException e) {
				continue;
			}
			
			this.binary();
		
			for (String name : names) {
				if (name.length() == 0) continue;
				
				File destination = null;
				try {
					destination = getFile(name, destinationFolder);					
				} catch (FtpProtocolException e) {
					//logger
					continue;
				}
			
				DWPArchiveProvider.addToArchive(connection, archiveTable, this.get(name), destination.getName(), destination.length());
				
				if (DWPArchiveProvider.compareWithArchive(connection, archiveTable, destination, destination.getName())) {
					deleteRemoteFile(name);
					destination.delete();
				};
			}		
		}		
		////////////////////////////////////////////////////////////////////////////////////////////
		if (connection != null) connection.close();
		closeServer();								

	}


	@Override
	public void deleteRemoteFile(String path) throws IOException {
		// TODO Auto-generated method stub
		//System.out.println("DELETE!!!");
		//logger_start
		this.issueCommand("DELE " + path);
		//logger_end
		
	}


	@Override
	public File getFile(String source, String destinationFolder) throws IOException {
		// TODO Auto-generated method stub
		
		InputStream streamReader = this.get(source);		
		File sourceFile = new File(source);
		
		String shortName = sourceFile.getName();		
		File destination = new File(destinationFolder + "/" + shortName);
		
		if (destination.exists()) destination.delete();//
		
		FileOutputStream fos = new FileOutputStream(destination);
		
		byte[] buffer = new byte[BUFFER_SIZE];
		
		while (true) {
			int readed = streamReader.read(buffer); 
			if (readed <= 0) break;
			fos.write(buffer, 0, readed);
		}
		fos.close();	
		//destination
		return destination;
	}

	
	public String[] getFileList(String remoteFolder) throws IOException {
		InputStream streamReader = nameList(remoteFolder);

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
	
		byte[] buffer = new byte[BUFFER_SIZE];
		
		
		while (true) {
			int readed = streamReader.read(buffer); 
			if (readed <= 0) break;
			bos.write(buffer, 0, readed);				
		}

		String[] names = bos.toString().split("\n");		
		bos.close();
		streamReader.close();
		return names; 
	}

	
}
