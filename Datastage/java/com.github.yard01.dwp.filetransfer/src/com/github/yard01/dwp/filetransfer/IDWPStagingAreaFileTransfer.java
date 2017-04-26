package com.github.yard01.dwp.filetransfer;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import com.github.yard01.dwp.db.DWPConnector;

public interface IDWPStagingAreaFileTransfer {
	
	public void start(Properties config) throws IOException, ClassNotFoundException, SQLException;
		
	public void stop();
	
	public void deleteRemoteFile(String path) throws IOException;
	
	public File getFile(String source, String destinationFolder) throws IOException;
	
	
}
