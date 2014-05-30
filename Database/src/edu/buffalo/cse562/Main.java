package edu.buffalo.cse562;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import edu.buffalo.cse562.queryParser.QueryParser;

public class Main {

	public static String DATA_ROOT_DIRECTORY = null;
	public static String SWAP_DIRECTORY = null;

	public static void main(String[] args) {
		List<File> sqlFilesList = new ArrayList<>();
		QueryParser queryParser = new QueryParser();
		
		for(int index=0; index<args.length;index++) {
			if(args[index].equals("--data")) {
				DATA_ROOT_DIRECTORY = args[index+1];
				index++;
			} else if(args[index].equals("--swap")) {
				SWAP_DIRECTORY = args[index+1];
				index++;
			} else {
				sqlFilesList.add(new File(args[index]));
			}
		}
		
		for(File sqlFile: sqlFilesList){
	    	String fileString = "";
	    	try{
	        	FileReader stream = new FileReader(sqlFile);
	        	CCJSqlParser parser = new CCJSqlParser(stream);
	        	Statement stmt;
	        	while((stmt = parser.Statement()) != null){
	        		cleanSwapDirectory();
	        		stmt.accept(queryParser);
	        	}
	    	}catch(IOException e){
	    		e.printStackTrace();
	    		System.err.print(fileString);
	    	}catch(ParseException e){
	    		System.err.print("SQL parse failed. Check your SQL file's syntax.");
	    	}
	    }
	}
	
	private static void cleanSwapDirectory() {
		for(File file: new File(SWAP_DIRECTORY).listFiles()) {
			file.delete();
		}
	}
}