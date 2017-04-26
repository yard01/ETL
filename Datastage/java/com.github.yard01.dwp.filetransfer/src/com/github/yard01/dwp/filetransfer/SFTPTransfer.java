package com.github.yard01.dwp.filetransfer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Vector;

import com.github.yard01.dwp.db.DWPConnector;
import com.github.yard01.dwp.filetransfer.archiver.DWPArchiveProvider;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class SFTPTransfer implements IDWPStagingAreaFileTransfer {
	
	private JSch jsch;
	private Session session;
	private Channel channel;
	
	
	public static void main(String[] args) {
		
		SFTPTransfer sftp = new SFTPTransfer();
		
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
		cfg.put("path", "/tmp/*.txt1");
		cfg.put("host", "10.93.72.225");			

		try {
			sftp.start(cfg);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	//@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

	//@Override
	public void start(Properties config) throws ClassNotFoundException,
			SQLException, IOException {
		// TODO Auto-generated method stub
		String timeout           = config.getProperty(DWPFileTransferProvider.TIMEOUT_PROPERTY);
		String destinationFolder = config.getProperty(DWPFileTransferProvider.DESTINATION_PROPERTY);
		File stopFile            = new File(config.getProperty(DWPFileTransferProvider.STOP_FLAG_PROPERTY));
		String archiveTable      = config.getProperty(DWPFileTransferProvider.ARCHIVE_TABLE_PROPERTY);
		String sourcePath        = config.getProperty(DWPFileTransferProvider.PATH_PROPERTY);
		
		String sourceDir         = sourcePath.substring(0, sourcePath.lastIndexOf("/")); //  source.getParent();
		
		jsch = new JSch();

		try {
			session = jsch.getSession(
					config.getProperty(DWPFileTransferProvider.LOGIN_PROPERTY),
					config.getProperty(DWPFileTransferProvider.HOST_PROPERTY));
			session.setConfig("StrictHostKeyChecking", "no");

			try {
				session.setTimeout(Integer.valueOf(timeout) * 1000);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				session.setTimeout(DWPFileTransferProvider.DEFAULT_TIMEOUT * 1000);
			}
			this.session.setPassword(config.getProperty(DWPFileTransferProvider.PASSWORD_PROPERTY));
			this.session.connect();
			
			Connection connection = DWPConnector.getConnection(config);

			this.channel = session.openChannel("sftp");
			this.channel.connect();
			
			//Vector<LsEntry> fileList;
			while (!stopFile.exists()) {
				DWPFileTransferProvider.pause();
				try {
					Vector<LsEntry> fileList = ((ChannelSftp)channel).ls(sourcePath);
					//((ChannelSftp)channel).
					for (LsEntry entry : fileList) {
						
						String remotePath = sourceDir +"/" + entry.getFilename();
						InputStream readStream = ((ChannelSftp)channel).get(remotePath);
																		
						File destination = null;
						
						try {
							destination = getFile(remotePath, destinationFolder);					
						} catch (IOException e) {
							//logger							
							continue;
						}						
						DWPArchiveProvider.addToArchive(connection, archiveTable, readStream, destination.getName(), destination.length());
						
						if (DWPArchiveProvider.compareWithArchive(connection, archiveTable, destination, destination.getName())) {
							deleteRemoteFile(remotePath);
							destination.delete();
						};
						
					}
				} catch (SftpException e) {
				// TODO Auto-generated catch block
					e.printStackTrace();
					continue;
										
					//throw new IOException(e.getMessage());
				}
				
			} //
			if (connection != null) connection.close();
			channel.disconnect();
			session.disconnect();

		} catch (JSchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new IOException(e.getMessage());
		}

	}

//	@Override
	public void deleteRemoteFile(String path) throws IOException {
		// TODO Auto-generated method stub
		try {
			((ChannelSftp)channel).rm(path);
		} catch (SftpException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new IOException(e.getMessage());
		}
	}

	//@Override
	public File getFile(String source, String destinationFolder)
			throws IOException {
		// TODO Auto-generated method stub
		File fileSource = new File(source);
		
		File fileResult = new File(destinationFolder + "/" + fileSource.getName());
		
		try {
			((ChannelSftp)this.channel).get(source, fileResult.getAbsolutePath());
		} catch (SftpException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new IOException(e.getMessage());
		}

		return fileResult;
	}

	
}
