package br.scmjoin;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class LogFile {
	BufferedWriter writer = null;
	String newLine;
	public LogFile() {
		
		try {
			//create a temporary file
			String timeLog = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
			timeLog = timeLog + ".txt";
			this.newLine = System.getProperty("line.separator");
			File logFile = new File(timeLog);

			// This will output the full path where the file will be written to...
			System.out.println(logFile.getCanonicalPath());

			this.writer = new BufferedWriter(new FileWriter(logFile));
		} catch (Exception e) {
            e.printStackTrace();
        } 
	}
	public LogFile(String filename) {
		
		try {
			//create a temporary file			
			this.newLine = System.getProperty("line.separator");
			File logFile = new File(filename);

			// This will output the full path where the file will be written to...
			System.out.println(logFile.getCanonicalPath());

			this.writer = new BufferedWriter(new FileWriter(logFile));
		} catch (Exception e) {
            e.printStackTrace();
        } 
	}
	public void writeLog (String line){
		try {	                        
            this.writer.write(line);
            this.writer.write(this.newLine);
            this.writer.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	public void closeLog() {
	    try {
            // Close the writer regardless of what happens...
            writer.close();
        } catch (Exception e) {
        	e.printStackTrace();
        }
	}

}
