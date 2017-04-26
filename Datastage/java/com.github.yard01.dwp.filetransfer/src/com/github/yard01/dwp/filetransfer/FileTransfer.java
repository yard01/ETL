package com.github.yard01.dwp.filetransfer;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import com.github.yard01.dwp.db.DWPConnector;
import com.github.yard01.dwp.filetransfer.archiver.DWPArchiveProvider;

public class FileTransfer implements IDWPStagingAreaFileTransfer {
	/*
	public static void main(String[] args) {
		Properties cfg = new Properties();
		cfg.put("db_driver", "com.ibm.db2.jcc.DB2Driver");
		cfg.put("db_connection", "jdbc:db2://SSUMOD:50000/DBDWH");		
		cfg.put("db_user", "dsadm");
		cfg.put("db_password", "dsadm");
		cfg.put("table", "ARCHIVE.ARCHIVE_DWP");
		cfg.put("destination", "E:/Tools");
		cfg.put("stopflag", "E:/Tools/stop.flag");		
		cfg.put("timeout", "50");		
		cfg.put("login", "root");
		cfg.put("password", "2wsx2WSX");
		cfg.put("path", "E:/DS_Project/?_2015*");
		cfg.put("host", "10.93.72.225");
		
		FileTransfer ft = new FileTransfer();
		try {
			ft.start(cfg);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	*/
	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void start(Properties config) throws IOException, SQLException, ClassNotFoundException {
		// TODO Auto-generated method stub
		String destinationFolder = config.getProperty(DWPFileTransferProvider.DESTINATION_PROPERTY);
		File stopFile            = new File(config.getProperty(DWPFileTransferProvider.STOP_FLAG_PROPERTY));
		String archiveTable      = config.getProperty(DWPFileTransferProvider.ARCHIVE_TABLE_PROPERTY);
		String sourcePath        = config.getProperty(DWPFileTransferProvider.PATH_PROPERTY);		
		File sourceFile          = new File(sourcePath);
		
		File sourceDir = sourceFile.getParentFile();
		final String fileMask = sourceFile.getName().replace(".", "\\.").replace("*", ".*").replace("?", ".");
		
		FileFilter filter = new FileFilter() {

			@Override
			public boolean accept(File pathname) {
				// TODO Auto-generated method stub
				return pathname.isFile() && pathname.getName().matches(fileMask);
			}
			
		};
		
		
		Connection connection    = DWPConnector.getConnection(config);
		
		while (!stopFile.exists()) {
			DWPFileTransferProvider.pause();
			
			File[] files = sourceDir.listFiles(filter);
			for (File file : files) {
				InputStream readStream = new FileInputStream(file);
				DWPArchiveProvider.addToArchive(connection, archiveTable, readStream, file.getName(), file.length());
				readStream.close();
				if (DWPArchiveProvider.compareWithArchive(connection, archiveTable, file, file.getName())) {
					deleteRemoteFile(file.getAbsolutePath());
				}				
			}
		}
		
		if (connection != null) connection.close();
	}

	@Override
	public void deleteRemoteFile(String path) {
		// TODO Auto-generated method stub
		new File(path).delete();
	}

	@Override
	public File getFile(String source, String destinationFolder)
			throws IOException {
		// TODO Auto-generated method stub
		
		return new File(source);
	}


}
