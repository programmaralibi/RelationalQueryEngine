package edu.buffalo.cse562;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

public class TableDataService {
	
	private String tableName;
	private List<String[]> tableData;
	private boolean isOriginal = true;
	
	final static String DATA_ROOT = Main.DATA_ROOT_DIRECTORY;
	final static String DELIMITER = Helper.DELIMITER;
	final static String EXTENSION = ".dat";
	private static int MODE = 1; // means data loaded in batchform 
	//private static int BATCH_SIZE = Helper.BATCH_SIZE;
	private static HashMap<String, Scanner> tableDataScannerMap = new HashMap<>();
	private static HashMap<String, Boolean> tableReadCompleteMap = new HashMap<>();
	private static HashMap <String, List<String[]>> tableDataBatches = new HashMap<>();
	
	public boolean isReadComplete() {
		return tableReadCompleteMap.get(tableName);
	}
	
	public void setReadComplete(boolean value) {
		tableReadCompleteMap.put(tableName, value);
	}
	
	public boolean isOriginal() {
		return isOriginal;
	}

	public TableDataService(String tableName, List<String[]> tableData) {
		this.tableName = tableName;
		this.tableData = tableData;
	}
	
	public TableDataService(String tableName) {
		this.tableName = tableName;
		if(Helper.existsTableDataService(tableName)) { 
			TableDataService previousTableDataService = Helper.getTableDataService(tableName);
			if(previousTableDataService.isOriginal) {
			//	this.tableData = previousTableDataService.getTableData();
			} else { 
			//	this.tableData = getTableDataFromFile();
			}
		} else { 
			//this.tableData = getTableData();
		}
		tableReadCompleteMap.put(tableName, false);
	}

	public String getTableName() {
		return tableName;
	}
	
	private static List<String[]> getNextBatch(String tableName) {
		tableName = tableName.toUpperCase();
		if(tableDataScannerMap.get(tableName) == null) {
			File file = new File(DATA_ROOT+File.separatorChar+tableName+EXTENSION);
			int lineSize = Helper.DEFAULT_LINE_SIZE;
			try {
				Scanner scanner = new Scanner(file);
				lineSize = scanner.nextLine().length() * 2;
				scanner.close();
			} catch (Exception e) {
				System.err.println("Error whule reading file : "+file.getName() + " -> "+e.getMessage());
			}
			try {
				Helper.BATCH_SIZE_MAP.put(tableName, Helper.BATCH_SIZE_IN_BYTES/lineSize);
				tableDataScannerMap.put(tableName, new Scanner(file));
			} catch (FileNotFoundException e) {
				throw new RuntimeException("Could not open file "+ tableName+ "." + e.getMessage());
			}
		} 
		Scanner scanner = tableDataScannerMap.get(tableName);
		if(!scanner.hasNextLine()) {
			tableReadCompleteMap.put(tableName, true);
			return null;
		}
		List<String[]> tableData = new ArrayList<>();
		for(int index=0; index<Helper.BATCH_SIZE_MAP.get(tableName) ; index++) {
			if(scanner.hasNextLine()) {
				String lineRead = scanner.nextLine();
				tableData.add(lineRead.split(DELIMITER));
			} else {
				tableReadCompleteMap.put(tableName, true);
//				resetAllInnerScanners(Helper.joinTablesList, Helper.joinTablesList.get(Helper.joinTablesList.indexOf(tableName)+1));
				break;
			}
		}
		if(!scanner.hasNextLine()) {
			tableReadCompleteMap.put(tableName, true);
		} 
		return tableData;
	}
	
	public List<String[]> getTableData() {
		List<String[]>tableData;
		if(!tableName.contains(Helper.subSelectTableName)) {
			tableData = getNextBatch(tableName);
		} else {
			tableData = getTableDataFromMemory();
		}
		this.tableData = tableData;
		Helper.tableDataServiceMap.put(tableName, this);
		return tableData;
	}
	
	private static List<String[]> getTableData(String tableName) {
		List<String[]> tableData = getNextBatch(tableName);
		return tableData; // A Batch of Table Data
	}
	
	public List<String[]> getTableDataFromMemory() {
		return tableData; // Only the data frommemory
	}
	
	
	public void resetTableScanner(String tableName) {
		Scanner scanner = createFileScanner(tableName);
		tableDataScannerMap.put(tableName, scanner);
		tableReadCompleteMap.put(tableName, false);
	}
	

	public void setTableData(List<String[]> tableData) {
		isOriginal = false;
		this.tableData = tableData;
	}

	public void printqueryResult(List<Integer> allColumnIndex) {
		for(String[] tuple : tableData) {
			for(int columnNumber : allColumnIndex) {
				System.out.print(tuple[columnNumber]+ "|");
			}
		}
	}
	
	private static List<List<String[]>>readData(List<String> tablesNameList) {
		List<List<String[]>> tableListData = new ArrayList<>();
		for(int index = 0; index<tablesNameList.size(); index++) {
			String tableName = tablesNameList.get(index);
			boolean dataAdded = false;
			for(int innerIndex = index+1; innerIndex<tablesNameList.size(); innerIndex++) {
				if(!tableReadCompleteMap.get(tablesNameList.get(index+1))) {
					tableListData.add(tableDataBatches.get(tableName));
					dataAdded = true;
					break;
				}
			}
			if(!dataAdded) {
				tableListData.add(getTableData(tableName));
			}
		}
		return tableListData;
	}
	
	public static void resetAllInnerScanners(List<String>tablesNameList, String firstTableName) {
		for(int index = tablesNameList.indexOf(firstTableName); index<=tablesNameList.size() - 1; index++) {
			String tableName = tablesNameList.get(index);
			if(tableDataScannerMap.get(tableName) == null) {
				throw new RuntimeException("Some error with scanner of "+ tablesNameList.get(index) + ".");
			}
			File file = new File(DATA_ROOT+File.separatorChar+tableName+EXTENSION);
			try {
				tableDataScannerMap.put(tableName, new Scanner(file));
				tableReadCompleteMap.put(tableName, false);
			} catch (FileNotFoundException e) {
				throw new RuntimeException("Could not open file "+ tableName+ "." + e.getMessage());
			}
		} 
	}
	
	public void resetTableScanner() {
		File file = new File(DATA_ROOT+File.separatorChar+tableName+EXTENSION);
		try {
			tableDataScannerMap.put(tableName, new Scanner(file));
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Could not open file "+ tableName+ "." + e.getMessage());
		}
		tableReadCompleteMap.put(tableName, false);
	}
	
	private List<String[]> getTableDataFromFile() {
		List<String[]> tableData = new ArrayList<String[]>();
		Scanner scanner = createFileScanner(tableName);
		while(scanner.hasNextLine()) {
			String[] tokens = scanner.nextLine().split(DELIMITER);
			tableData.add(tokens);
		}
		scanner.close();
		return tableData;
	}
	
	private Scanner createFileScanner(String tableName) {
		Scanner scanner;
		File file = new File(DATA_ROOT+File.separatorChar+tableName+EXTENSION);
		if(!file.exists()) {
			throw new RuntimeException("File "+ tableName+ " not found.");
		}
		try {
			scanner = new Scanner(file);
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Could not open file "+ tableName+ "." + e.getMessage());
		}
		return scanner;
	}
	
	
	
}
