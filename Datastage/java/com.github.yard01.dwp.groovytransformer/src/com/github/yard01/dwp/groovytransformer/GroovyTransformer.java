package com.github.yard01.dwp.groovytransformer;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

//import com.ascentialsoftware.jds.Row;
import com.ascentialsoftware.jds.Stage;

public class GroovyTransformer extends Stage {
	
	public static final String SELF = "GroovyTfm";
	
	private String script;
	private int outputStatus = OUTPUT_STATUS_READY;
	
	public void initialize() {
		
		trace("TableSource.initialize");
		script = this.getUserProperties();
		
	}

	public void terminate() {
		trace("TableSource.terminate");	
	}
  
	public void setOutputStatus(int outputStatus) {
		this.outputStatus = outputStatus;
	}
	
	public int process() {

//		Row inputRow = this.readRow();		
//		if (inputRow == null) return OUTPUT_STATUS_END_OF_DATA; 
		
		Binding binding = new Binding();
		GroovyShell shell = new GroovyShell(binding);
		shell.setVariable(SELF, this);					
		shell.evaluate(script);
	    	
		return this.outputStatus; 										
	}
		
	
}
