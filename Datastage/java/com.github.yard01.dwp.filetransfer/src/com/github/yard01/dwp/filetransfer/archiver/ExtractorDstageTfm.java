package com.github.yard01.dwp.filetransfer.archiver;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import com.ascentialsoftware.jds.Row;
import com.ascentialsoftware.jds.Stage;
import com.github.yard01.dwp.db.DWPConnector;
import com.github.yard01.dwp.filetransfer.DWPFileTransferProvider;

public class ExtractorDstageTfm extends Stage {
	private	static Map<String, String> columns = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
		
	static {
		columns.put("DB_DRIVER", 		DWPFileTransferProvider.DB_DRIVER_PROPERTY);		
		columns.put("DB_CONNECTION",	DWPFileTransferProvider.DB_CONNECTION_PROPERTY);
		columns.put("DB_USER",			DWPFileTransferProvider.DB_USER_PROPERTY); 
		columns.put("DB_PASSWORD", 		DWPFileTransferProvider.DB_PASSWORD_PROPERTY);
		columns.put("TABLE", 			DWPFileTransferProvider.ARCHIVE_TABLE_PROPERTY); 
		columns.put("DESTINATION", 		DWPFileTransferProvider.DESTINATION_PROPERTY);
		columns.put("FILES",	 		DWPFileTransferProvider.PATH_PROPERTY);	
	}
	
	public static final String ERROR_STRING = "E";
	
	
	String result = "";
//	Logger logger;
	
	public void initialize() {
		
		trace("TableSource.initialize");		
	}

	public void terminate() {
		trace("TableSource.terminate");	
	}
  
	public int process() {
		//Reading row
		Row inputRow = this.readRow();
		
		if (inputRow == null) return OUTPUT_STATUS_END_OF_DATA; //
				
		Properties cfg = new Properties();
		
		for (int i = 0; i < inputRow.getColumnCount(); i++) {
			String columnName = inputRow.getColumn(i).getName();
			cfg.put(columns.get(columnName), inputRow.getValueAsString(i));
		}
		
		try {
			extractFiles(cfg);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			result = ERROR_STRING;
		}
		// result string in our Job 
		Row outputRow = createOutputRow();  		
		outputRow.setValueAsString(0, result);		
		writeRow(outputRow);
		
		return OUTPUT_STATUS_READY; 
	}
	
	protected static void extractFiles(Properties config) throws ClassNotFoundException, SQLException, IOException {
		String path = config.getProperty(DWPFileTransferProvider.PATH_PROPERTY);
		File pathFile = new File(path);
		String recordName = pathFile.getName();
		String destinationFolder = pathFile.getParent();
		Connection connection = null;
		connection = DWPConnector.getConnection(config);
		
		DWPArchiveProvider.getSomeFilesFromArchive(connection, config.getProperty(DWPFileTransferProvider.ARCHIVE_TABLE_PROPERTY), recordName, destinationFolder);

		if (connection != null) connection.close();

	}

}
