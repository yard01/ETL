package com.github.yard01.dwp.filetransfer.archiver;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.commons.io.FileUtils;

public class DWPArchiveProvider {
	public static final String TMP_SUFFIX = ".tmp";

	public static final int BUFFER_SIZE = 65536;
	public static void addToArchive(Connection connection, String tableName, InputStream readStream, String recordName, long bytesTotal) throws SQLException {
		
		PreparedStatement stmt = connection.prepareStatement("UPDATE " + tableName + " SET CURR_F = 0 WHERE FILE_NAME = ?"); // .createStatement();
		stmt.setString(1, recordName);
		stmt.executeUpdate();
		connection.commit();
		/////////////////////////////////////////////////////////////////////

		stmt = connection.prepareStatement("INSERT INTO " + tableName + " (FILE_NAME, FILE_SIZE, CURR_F, PPN_DT, FILE_BODY) VALUES (?, ?, ?, ?, ?)");
		stmt.setString(1, recordName);
		stmt.setLong(2, bytesTotal);
		stmt.setInt(3, 1);		
		java.util.Date today = new java.util.Date();
		stmt.setTimestamp(4, new java.sql.Timestamp(today.getTime()));
		stmt.setBinaryStream(5, readStream);
		stmt.execute();		
		connection.commit();		
	}
	
	public static File getFromArchive(Connection connection, String tableName, String recordName, String destination) throws SQLException, IOException {
		
		PreparedStatement stmt = connection.prepareStatement("SELECT LENGTH(FILE_BODY) AS FSIZE, FILE_BODY FROM " + tableName + " WHERE CURR_F = 1 AND FILE_NAME = ?"); 
		
		
		stmt.setString(1, recordName);
		ResultSet result = stmt.executeQuery();
		
		if (result.next()) {
			long bytesTotal = result.getLong("FSIZE");			
			InputStream is = result.getBinaryStream("FILE_BODY");
			File file = new File(destination);
			OutputStream fos = new FileOutputStream(file);
			DataOutputStream dos = new DataOutputStream(fos);
		
			byte[] data = new byte[BUFFER_SIZE];		
			long bytesReadTotal = 0;
			while (bytesReadTotal < bytesTotal) {
				int bytesRead = is.read(data);				
				bytesReadTotal += bytesRead; 
				dos.write(data, 0, bytesRead);								
			}
			dos.close();
			fos.close();
			return file;
		}
		return null;
		/////////////////////////////////////////////////////////////////////
	
	}
	

	public static void getSomeFilesFromArchive(Connection connection, String tableName, String recordName, String destinationFolder) throws SQLException, IOException {
		
		PreparedStatement stmt = connection.prepareStatement("SELECT FILE_NAME, LENGTH(FILE_BODY) AS FSIZE, FILE_BODY FROM " + tableName + " WHERE CURR_F = 1 AND FILE_NAME like ?"); 
			
		stmt.setString(1, recordName);
		ResultSet result = stmt.executeQuery();

		
		while (result.next()) {

			String fileName = result.getString("FILE_NAME");

			long bytesTotal = result.getLong("FSIZE");			
			InputStream is = result.getBinaryStream("FILE_BODY");
			File file = new File(destinationFolder+"/"+fileName);
			

			OutputStream fos = new FileOutputStream(file);
			DataOutputStream dos = new DataOutputStream(fos);
		
			byte[] data = new byte[BUFFER_SIZE];		
			long bytesReadTotal = 0;
			while (bytesReadTotal < bytesTotal) {
				int bytesRead = is.read(data);				
				bytesReadTotal += bytesRead; 
				dos.write(data, 0, bytesRead);								
			}
			dos.close();
			fos.close();
		}
		
		/////////////////////////////////////////////////////////////////////
	
	}

	
	
	public static boolean compareWithArchive(Connection connection, String archiveTable, File file, String name) throws SQLException, IOException {
		// TODO Auto-generated method stub
		File tmp_archive = DWPArchiveProvider.getFromArchive(connection, archiveTable, name, file.getAbsolutePath()+ TMP_SUFFIX);
		boolean result = FileUtils.contentEquals(tmp_archive, file);
		tmp_archive.delete();
		
		return result;
	}
}
