package com.github.yard01.dwp.filetransfer;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import com.ascentialsoftware.jds.Row;
import com.ascentialsoftware.jds.Stage;

public class TransferDstageTfm extends Stage {
	private	static Map<String, String> columns = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
		
	static {
		columns.put("DB_DRIVER", 		DWPFileTransferProvider.DB_DRIVER_PROPERTY);		
		columns.put("DB_CONNECTION",	DWPFileTransferProvider.DB_CONNECTION_PROPERTY);
		columns.put("DB_USER",			DWPFileTransferProvider.DB_USER_PROPERTY); 
		columns.put("DB_PASSWORD", 		DWPFileTransferProvider.DB_PASSWORD_PROPERTY);
		columns.put("TABLE", 			DWPFileTransferProvider.ARCHIVE_TABLE_PROPERTY); 
		columns.put("DESTINATION", 		DWPFileTransferProvider.DESTINATION_PROPERTY);
		columns.put("STOPFLAG", 		DWPFileTransferProvider.STOP_FLAG_PROPERTY);
		columns.put("FTP_TIMEOUT", 		DWPFileTransferProvider.TIMEOUT_PROPERTY);
		columns.put("FTP_LOGIN", 		DWPFileTransferProvider.LOGIN_PROPERTY);
		columns.put("FTP_PASSWORD", 	DWPFileTransferProvider.PASSWORD_PROPERTY);
		columns.put("FTP_PATH", 		DWPFileTransferProvider.PATH_PROPERTY);
		columns.put("FTP_HOST",			DWPFileTransferProvider.HOST_PROPERTY);
		columns.put("PROTOCOL",			DWPFileTransferProvider.PROTOCOL_PROPERTY);
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
			//this.info(columns.get(columnName)+" = "+inputRow.getValueAsString(i));
		}
		
		IDWPStagingAreaFileTransfer transfer = DWPFileTransferProvider.createTransfer(cfg);
		
		try {
			transfer.start(cfg);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			result = ERROR_STRING;
			e.printStackTrace();
		}
		
		
		// result string in our Job 
		Row outputRow = createOutputRow();  		
		outputRow.setValueAsString(0, result);		
		writeRow(outputRow);
		
		return OUTPUT_STATUS_READY; 
	}

}
