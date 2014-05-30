
package edu.buffalo.cse562;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Scanner;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import edu.buffalo.cse562.queryParser.QueryParser;

public class Helper {
	
	public static Map<String, TableDataService> tableDataServiceMap = new HashMap<>(); 
	private static TableDataModelling expressionVisitor = new TableDataModelling();  //Table Data Modelling implements Expressionvisitor
	private static boolean printPipe = false;
	private static boolean columnPrinted = false;
	public static final String subSelectTableName = "SUBSELECT_TABLE";
	private static HashMap<String, String[]> groupsMap = new HashMap<>();   // Map with Keys : grouping Parameters & Value : Group where Group is List of Tuples
	public static List<String> joinTablesList;
	public static final String PRODUCT_TABLE_IDENTIFIER = "PRODUCT_";
	private static boolean writeToFile = false;
	final static String SWAP_DIRECTORY = Main.SWAP_DIRECTORY;
	final static String DEFAULT_DATA_TYPE = "DOUBLE";
//	final static int BATCH_SIZE = 500; 				// reading 1 MB of data at a time from a file
	final static String DELIMITER = "\\|";
    static HashMap <String, Integer> BATCH_SIZE_MAP = new HashMap<>();	
    static int DEFAULT_LINE_SIZE = 1000;
    static int BATCH_SIZE_IN_BYTES = 200 * 1024;


	
	static final String ORDER_BY_VIEW_IDENTIFIER = "INTERMEDIATE_RESULT.dat";
	static final String INTERMEDIATE_FILE_IDENTIFIER = SWAP_DIRECTORY + File.separatorChar + "INTERMEDIATE_ORDERBY_VIEW";
	static final String INTERMEDIATE_FILE_EXTENSION = ".dat";
	private static int INTERMEDIATE_FILE_COUNT = 0;

	
	
	public static boolean isWriteToFile() {
		return writeToFile;
	}

	public static void setWriteToFile(boolean writeToFile) {
		Helper.writeToFile = writeToFile;
	}

	
	public static TableDataModelling getExpressionVisitor() {
		return expressionVisitor;
	}

	public static void setExpressionVisitor(TableDataModelling expressionVisitor) {
		Helper.expressionVisitor = expressionVisitor;
	}
	
	public static TableDataService getTableDataService(String tableName) {
		tableName = tableName.toUpperCase();
		TableDataService tableDataService = tableDataServiceMap.get(tableName);
		if (tableDataServiceMap.get(tableName) == null) {
			tableDataService = new TableDataService(tableName);
			tableDataServiceMap.put(tableName, tableDataService);
		}
		return tableDataService;
	}
	
	public static boolean existsTableDataService(String tableName) {
		if (tableDataServiceMap.get(tableName.toUpperCase()) == null) {
			return false;
		}
		return true;
	}

	public static void setTableDataService(String tableName, TableDataService tableDataService) {
		Helper.tableDataServiceMap.put(tableName.toUpperCase(), tableDataService);
	}

	public static List<String> getColumnNames(TableSchema tableSchema) {
		List<ColumnDefinition> columnDefinitions = tableSchema.getColumnDefinitions();
		List<String> columnNames = new ArrayList<String>();
		for(ColumnDefinition columnDefinition : columnDefinitions) {
			columnNames.add(columnDefinition.getColumnName());
		}
		return columnNames;
	}
		
	public static List<String[]> sortTableData (List<String[]> tableData, String tableName, List<OrderByElement> sortingParameterExpression) {
		TableSchema tableSchema = TableMetaDataService.createInstance().getTableSchema(tableName);
		ListIterator<String[]> tupleIterator = tableData.listIterator();
		List<String[]> sortedTuples = new ArrayList<String[]>();
		int accumulatorType;
		
		boolean updatedAccumulator = false;
		List<String> columnNames = Helper.getColumnNames(tableSchema);
		List<ColumnDefinition> columnDefinitions = tableSchema.getColumnDefinitions();
		List<String> columnDataTypes = new ArrayList<>();
		for(ColumnDefinition columnDefinition : columnDefinitions) {
			columnDataTypes.add(columnDefinition.getColDataType().getDataType());
		}

        while(tupleIterator.hasNext()){
        	String[] tuple = tupleIterator.next();
        	if(tuple.length == 0) { // Blank line may cause a blank tuple
        		break;
        	} else if(tuple.length == 1 && tuple[0].equals("")) {
        		break;
        	}
        	
			if(sortedTuples.size() == 0) {
				sortedTuples.add(tuple);
				continue;
			}
			ListIterator<String[]> sortedListIterator = sortedTuples.listIterator();
			boolean added = false;
			while(sortedListIterator.hasNext()) {
				Iterator<OrderByElement> sortingParameterIterator = sortingParameterExpression.iterator();
				String [] alreadySorted = sortedListIterator.next();	
				while(sortingParameterIterator.hasNext()) {
					OrderByElement orderByElement = sortingParameterIterator.next();
					boolean isAsc = orderByElement.isAsc();
					String currentSortingParameter = orderByElement.getExpression().toString();
					int indexOfSortingParam = columnNames.indexOf(currentSortingParameter);
					String dataType = columnDataTypes.get(indexOfSortingParam);
					if(dataType.equalsIgnoreCase("integer")||dataType.equalsIgnoreCase("int")||dataType.equalsIgnoreCase("long")) {
						boolean condition = new Long(tuple[indexOfSortingParam]) < new Long(alreadySorted[indexOfSortingParam]);
						if(condition && isAsc) {
							sortedListIterator.previous();
							sortedListIterator.add(tuple);
							added = true;
							break;
						} else if(new Long(tuple[indexOfSortingParam]) > new Long(alreadySorted[indexOfSortingParam])) {
							break;
						}
					} else if(dataType.equalsIgnoreCase("string") || dataType.equalsIgnoreCase("char")) {
						boolean condition = tuple[indexOfSortingParam].compareTo(alreadySorted[indexOfSortingParam]) < 0;
						if(condition && isAsc) {
							sortedListIterator.previous();
							sortedListIterator.add(tuple);
							added = true;
							break;
						} else if(tuple[indexOfSortingParam].compareTo(alreadySorted[indexOfSortingParam]) > 0) {
							break;
						}
					} else if(dataType.equalsIgnoreCase("date")) {
						DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
						boolean condition;
						try {
							if(tuple[indexOfSortingParam] == null || alreadySorted[indexOfSortingParam]== null) {
								condition = true;
							} else {
								condition = dateFormat.parse(tuple[indexOfSortingParam]).compareTo(dateFormat.parse(alreadySorted[indexOfSortingParam])) < 0;
							}
						if(condition && isAsc) {
							sortedListIterator.previous();
							sortedListIterator.add(tuple);
							added = true;
							break;
						} else if(tuple[indexOfSortingParam].compareTo(alreadySorted[indexOfSortingParam]) > 0) {
							break;
						}
						} catch (ParseException e) {
							System.err.print("Parseing error in date.");
						}
					}
				}
				if(added) break;
			}
			if(!added) {
				sortedListIterator.add(tuple);
			}
		}
        return sortedTuples;
	}
	
	public static class MyList extends ArrayList<String> {
		private List<String> parameterList;
		
		public MyList(List<String> parameterList) {
			this.parameterList = parameterList;
		}

		public boolean equals(MyList object) {
			for(int index=0 ; index< parameterList.size(); index++) {
				if(!parameterList.get(index).equals(object.parameterList.get(index))) {
					return false;
				}
			}
			return true;
		}
	}
	
	public static void processQuery(PlainSelect plainSelect) {
		List<Expression> groupByExpressionList = plainSelect.getGroupByColumnReferences();
		List<OrderByElement> orderByExpressionList = plainSelect.getOrderByElements();
		
		Expression whereExpression = plainSelect.getWhere();
		List<Join> joinsList = plainSelect.getJoins();
		List<SelectExpressionItem> selectItemExpressionList = plainSelect.getSelectItems();
		String tableName = null;
		boolean isOrderBy = false;
		
		//plainSelect.get
		if(plainSelect.getFromItem() instanceof Table) {
			tableName = ((Table)plainSelect.getFromItem()).getName().toUpperCase();
			expressionVisitor.setTableName(tableName);
		} else if(plainSelect.getFromItem() instanceof SubSelect) {
			tableName = Helper.subSelectTableName;
			expressionVisitor.setTableName(tableName);
			//plainSelect.getFromItem().accept(new QueryParser());
		}
		//String tableName = tableSchema.getTableName();
		TableMetaDataService tableMetaDataService = TableMetaDataService.createInstance();
		TableSchema tableSchema;
		List<String[]> tableData;

		if(joinsList!=null) {
			tableName = GroupByHelper.processJoins(joinsList, plainSelect.getFromItem(), whereExpression);
			createTableSchemaForJoins(joinsList, plainSelect.getFromItem()); // TableSchema is created for Product 
			tableData = tableDataServiceMap.get(tableName).getTableDataFromMemory();
			tableSchema = tableMetaDataService.getTableSchema(tableName);
			expressionVisitor.setTableName(tableName);
		} else {
			tableSchema = tableMetaDataService.getTableSchema(tableName);
			if(tableName.contains(subSelectTableName)) {
				tableData = tableDataServiceMap.get(tableName).getTableDataFromMemory();
			} else {
				tableData = tableDataServiceMap.get(tableName).getTableData();
			}
		}
		
		
		if(orderByExpressionList != null) {
			isOrderBy = true;
			writeToFile = true;
			createTableSchema(ORDER_BY_VIEW_IDENTIFIER, selectItemExpressionList, tableName);
		}

		do {
			if(tableData == null) {
				throw new RuntimeException("TableData null for table :"+tableName);
			}
			
			
			if(groupByExpressionList != null) {
			    GroupByHelper.processGroups(plainSelect, selectItemExpressionList, tableName, whereExpression, tableData);
		     } else {
				processTuples(selectItemExpressionList, tableData, whereExpression);
		     }
			

			// ***************** These are conditions to get next batch of DATA
			if(joinsList!=null) {
				tableName = GroupByHelper.processJoins(joinsList, plainSelect.getFromItem(), whereExpression);
				tableData = tableDataServiceMap.get(tableName).getTableDataFromMemory();
				tableSchema = tableMetaDataService.getTableSchema(tableName);
				expressionVisitor.setTableName(tableName);
			} else {
				tableData = tableDataServiceMap.get(tableName).getTableData();
			}
		     
		}while(tableData !=null);
		
		if(groupByExpressionList != null) {
			GroupByHelper.printGroups();
		}
		
		if(isOrderBy) {
			writeToFile = false;
			sortTableData(ORDER_BY_VIEW_IDENTIFIER, orderByExpressionList);
		}
		


/*		if (groupByExpressionList != null) {
			List<List<String[]>> tableDataGroups = createTableDataGroups(tableData, tableSchema, groupByExpressionList);
			for(List<String[]> newTableDataGroup : tableDataGroups ) {
				printResult(selectItemExpressionList, newTableDataGroup);
			}
		} else {
			printResult(selectItemExpressionList, tableData);
		}
*/	}
	
	
	private static void createTableSchemaForJoins(List<Join> joinsList, FromItem fromItem) {
		
		StringBuffer productTableName = new StringBuffer("PRODUCT");
		List<String> tableNamesList = new ArrayList<>();
		String leftTableName = ((Table)fromItem).getName().toUpperCase();
		tableNamesList.add(leftTableName);
		productTableName.append("_"+leftTableName);

		for (Join join : joinsList) {
			if(join.getRightItem() instanceof Table) {
				String rightTableName = ((Table)join.getRightItem()).getName().toUpperCase();
				tableNamesList.add(rightTableName);
				productTableName.append("_"+rightTableName);
			}
		}
		
		TableMetaDataService tableMetaDataService = TableMetaDataService.createInstance();
		List<ColumnDefinition> productColumnDefinitionList = new ArrayList<>();
		for(String tableName : tableNamesList) {
			for(ColumnDefinition columnDefinition : tableMetaDataService.getTableSchema(tableName).getColumnDefinitions()) {
				ColumnDefinition colDef = new ColumnDefinition();
				colDef.setColumnName(tableName+"."+columnDefinition.getColumnName());
				colDef.setColDataType(columnDefinition.getColDataType());
				productColumnDefinitionList.add(colDef);
			}
		}
		
		TableSchema tableSchema = new TableSchema(productTableName.toString(), productColumnDefinitionList); 
		tableMetaDataService.setTableSchema(productTableName.toString(), tableSchema);
	}

	private static void createTableSchema(String tableName, List<SelectExpressionItem> selectItemExpressionList, String referenceTableName) {
		TableSchema tableSchema = new TableSchema(tableName);
		TableMetaDataService tableMetaDataService = TableMetaDataService.createInstance();
		TableSchema referenceTableSchema = tableMetaDataService.getTableSchema(referenceTableName);
		List<ColumnDefinition> colDefinitionList = new ArrayList<>();

		for(SelectExpressionItem item : selectItemExpressionList) {
			ColumnDefinition columnDefinition = null;
			String columnName = null;
			if(item.getExpression() instanceof Function) {
	        	Function function = (Function)item.getExpression();
				if(function.isAllColumns()) { // Means COUNT(*)
					columnName = "*";
				} else {
					columnName = ((Function)item.getExpression()).getParameters().getExpressions().get(0).toString();
				}
			} else {
				String colTableName = ((Column)item.getExpression()).getTable().getName();
				columnName = colTableName + "." + ((Column)item.getExpression()).getColumnName();
			}
			
			if(item.getAlias() != null) {
				columnName = item.getAlias();
			}
			
			for(ColumnDefinition referenceColumnDefinition : referenceTableSchema.getColumnDefinitions()) {
				if(referenceColumnDefinition.getColumnName().equalsIgnoreCase(columnName)) {
					columnDefinition = referenceColumnDefinition;
					break;
				}
			}
			if(columnDefinition == null) {
				columnDefinition = new ColumnDefinition();
				columnDefinition.setColumnName(columnName);
				ColDataType colDataType = new ColDataType();
				colDataType.setDataType(DEFAULT_DATA_TYPE);
				columnDefinition.setColDataType(colDataType);
			}
			colDefinitionList.add(columnDefinition);
		}
		
		tableSchema.setcolumnDefinitions(colDefinitionList);
		tableMetaDataService.setTableSchema(tableName, tableSchema);
	}

	
	public static void sortTableData (String tableName, List<OrderByElement> sortingParameterExpression) {
	
		TableSchema tableSchema = TableMetaDataService.createInstance().getTableSchema(tableName);
		List<String> columnNames = Helper.getColumnNames(tableSchema);
		List<String> columnDataTypes = new ArrayList<>();
		for(ColumnDefinition columnDefinition : tableSchema.getColumnDefinitions()) {
			columnDataTypes.add(columnDefinition.getColDataType().getDataType());
		}

		List<String[]> sortedTuplesAfterMergeSort = new ArrayList<String[]>();
		List<String[]> sortedTuples = null;


		File inputFile = new File(SWAP_DIRECTORY + File.separatorChar + ORDER_BY_VIEW_IDENTIFIER);
		int lineSize = DEFAULT_LINE_SIZE;
		try {
			Scanner scanner = new Scanner(inputFile);
			lineSize = scanner.nextLine().length() * 2;
			scanner.close();
		} catch (Exception e) {
			System.err.println("Error whule reading file : "+inputFile.getName() + " -> "+e.getMessage());
		}

		Helper.BATCH_SIZE_MAP.put(tableName, BATCH_SIZE_IN_BYTES/lineSize);
		boolean readComplete = false;
		
		try {
			Scanner scanner = new Scanner(inputFile);
			while (true) {
				List<String[]> tableData = new ArrayList<String[]>();

				int linesRead = 0;
				PrintWriter outputFile = new PrintWriter(new FileWriter(new File(INTERMEDIATE_FILE_IDENTIFIER+INTERMEDIATE_FILE_COUNT+INTERMEDIATE_FILE_EXTENSION)));
	
				while(linesRead < BATCH_SIZE_MAP.get(ORDER_BY_VIEW_IDENTIFIER)) {
					String line = scanner.nextLine();
					linesRead ++;
					if(line.length() > 1) {
						String[] tokens = line.split(DELIMITER);
						tableData.add(tokens);
					}
					if(line == null || !scanner.hasNext()) {
						readComplete = true;
						break;
					}
				}
	
				sortedTuples = sorting(tableData, columnNames, columnDataTypes, sortingParameterExpression);
	
				ListIterator<String[]> sortedTupleIterator = sortedTuples.listIterator();
				while (sortedTupleIterator.hasNext()) {
					String[] tuple = sortedTupleIterator.next();
					for(String column : tuple) {
						String value = column + "|";
						outputFile.write(value);
					}
					outputFile.write("\n");
				}
				outputFile.close();
				INTERMEDIATE_FILE_COUNT++;
				if(readComplete) {
					break;
				}
			}
			scanner.close();
			merge(inputFile, tableName, sortingParameterExpression);
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Could not open file Intermediate_File_To_Sort.txt " + e.getMessage());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	public static List<String[]> sorting(List<String[]> tableData, List<String> columnNames, List<String> columnDataTypes, List<OrderByElement> sortingParameterExpression)
	{
		ListIterator<String[]> tupleIterator = tableData.listIterator();

		List<String[]> sortedTuples = new ArrayList<>();
		while(tupleIterator.hasNext()){
			String[] tuple = tupleIterator.next();
			if(tuple.length == 0) { // Blank line may cause a blank tuple
				break;
			} else if(tuple.length == 1 && tuple[0].equals("")) {
				break;
			}

			if(sortedTuples.size() == 0) {
				sortedTuples.add(tuple);
				continue;
			}
			ListIterator<String[]> sortedListIterator = sortedTuples.listIterator();
			boolean added = false;
			while(sortedListIterator.hasNext()) {
				String [] alreadySorted = sortedListIterator.next();

				Iterator<OrderByElement> sortingParameterIterator = sortingParameterExpression.iterator();

				while(sortingParameterIterator.hasNext()) {
					OrderByElement orderByElement = sortingParameterIterator.next();
					boolean isAsc = orderByElement.isAsc();
					String currentSortingParameter = orderByElement.getExpression().toString();
					if(currentSortingParameter.indexOf(".") != -1) {
						String parameterTableName = currentSortingParameter.split("\\.")[0].toUpperCase();
						String parameterColumnName = currentSortingParameter.split("\\.")[1];
						currentSortingParameter = parameterTableName + "." + parameterColumnName;
					}
					
					int indexOfSortingParam = columnNames.indexOf(currentSortingParameter);
					String dataType = columnDataTypes.get(indexOfSortingParam);
					if(dataType.equalsIgnoreCase("integer")||dataType.equalsIgnoreCase("int")||dataType.equalsIgnoreCase("long")) {
						boolean conditionAsc = new Long(tuple[indexOfSortingParam]) < new Long(alreadySorted[indexOfSortingParam]);
						boolean conditionDesc = new Long(tuple[indexOfSortingParam]) > new Long(alreadySorted[indexOfSortingParam]);
						if((conditionAsc && isAsc) || (conditionDesc && !isAsc)) {
							sortedListIterator.previous();
							sortedListIterator.add(tuple);
							added = true;
							break;
						} else if(new Long(tuple[indexOfSortingParam]) > new Long(alreadySorted[indexOfSortingParam])) {
							break;
						}
					} else if(dataType.equalsIgnoreCase("DOUBLE")) {
						boolean conditionAsc = new Double(tuple[indexOfSortingParam]) < new Double(alreadySorted[indexOfSortingParam]);
						boolean conditionDesc = new Double(tuple[indexOfSortingParam]) > new Double(alreadySorted[indexOfSortingParam]);
						if((conditionAsc && isAsc) || (conditionDesc && !isAsc)) {
							sortedListIterator.previous();
							sortedListIterator.add(tuple);
							added = true;
							break;
						} else if(new Double(tuple[indexOfSortingParam]) > new Double(alreadySorted[indexOfSortingParam])) {
							break;
						}
					} else if(dataType.equalsIgnoreCase("string") || dataType.equalsIgnoreCase("char") || dataType.equalsIgnoreCase("varchar")) {
						boolean conditionAsc = tuple[indexOfSortingParam].compareTo(alreadySorted[indexOfSortingParam]) < 0;
						boolean conditionDesc = tuple[indexOfSortingParam].compareTo(alreadySorted[indexOfSortingParam]) > 0;
						if((conditionAsc && isAsc) || (conditionDesc && !isAsc)) {
							sortedListIterator.previous();
							sortedListIterator.add(tuple);
							added = true;
							break;
						} else if(tuple[indexOfSortingParam].compareTo(alreadySorted[indexOfSortingParam]) > 0) {
							break;
						}
					} else if(dataType.equalsIgnoreCase("date")) {
						DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
						boolean conditionAsc, conditionDesc;
						try {
							if(tuple[indexOfSortingParam] == null || alreadySorted[indexOfSortingParam]== null) {
								conditionAsc = conditionDesc = true; // Why we have used this ?????
							} else {
								conditionAsc = dateFormat.parse(tuple[indexOfSortingParam]).compareTo(dateFormat.parse(alreadySorted[indexOfSortingParam])) < 0;
								conditionDesc = dateFormat.parse(tuple[indexOfSortingParam]).compareTo(dateFormat.parse(alreadySorted[indexOfSortingParam])) > 0;
							}
							if((conditionAsc && isAsc) || (conditionDesc && !isAsc)) {
								sortedListIterator.previous();
								sortedListIterator.add(tuple);
								added = true;
								break;
							} else if(tuple[indexOfSortingParam].compareTo(alreadySorted[indexOfSortingParam]) > 0) {
								break;
							}
						} catch (ParseException e) {
							System.err.print("Parsing error in date.");
						}
					}
				}
				if(added) break;
			}
			if(!added) {
				sortedListIterator.add(tuple);
			}
		}
		return sortedTuples;

	}
	
	private static void merge(File inputFile, String tableName, List<OrderByElement> sortingParameterExpression) {
		// Merge Sort
		List<String[]> firstRows = new ArrayList<String[]>();
		List<Integer> firstRowFileNumber = new ArrayList<>();

		List<Scanner> inputScanners = createScanners(INTERMEDIATE_FILE_IDENTIFIER, INTERMEDIATE_FILE_COUNT);
		for(Scanner inputScanner : inputScanners) {
			if(inputScanner.hasNextLine()) {
				firstRows.add(inputScanner.nextLine().split(DELIMITER));
				firstRowFileNumber.add(inputScanners.indexOf(inputScanner));
			}
		}
		
		TableSchema tableSchema = TableMetaDataService.createInstance().getTableSchema(tableName);
		List<String> columnNames = Helper.getColumnNames(tableSchema);
		List<String> columnDataTypes = new ArrayList<>();
		for(ColumnDefinition columnDefinition : tableSchema.getColumnDefinitions()) {
			columnDataTypes.add(columnDefinition.getColDataType().getDataType());
		}

		do {
			List<String[]> copyMergerSort = new ArrayList<>(firstRows);
			
			String[] tuple = sorting(firstRows, columnNames, columnDataTypes, sortingParameterExpression).get(0);
			int index = copyMergerSort.indexOf(tuple); // Tells the file number of intermediate file.
			firstRows.remove(index);

			for(String column: tuple) {
				System.out.print(column+"|");
			}
			System.out.println();

			String line = null;
			if(inputScanners.get(firstRowFileNumber.get(index)).hasNext()) {
				line = inputScanners.get(firstRowFileNumber.get(index)).nextLine();
			}
			if(line!= null && line.length() > 0 ) {
				String[] tokens = line.split(DELIMITER);
				firstRows.add(index, tokens);
			} else {
				firstRowFileNumber.remove(index);
			}
		} while(firstRows.size() > 0);
	}
	
	public static List<Scanner> createScanners(String fileName, int count) {
		List<Scanner> inputScannerList = new ArrayList<>();	
		for(int index=0; index < count; index++) {
			try {
				Scanner scanner = new Scanner(new File(INTERMEDIATE_FILE_IDENTIFIER + index + INTERMEDIATE_FILE_EXTENSION));
				inputScannerList.add(scanner);
			} catch (FileNotFoundException e) {
				System.out.println(e.getMessage());
			}
		}
		return inputScannerList;
	}
	

	private static void processTuples(List<SelectExpressionItem> selectItemExpressionList, List<String[]> tableData, Expression whereExpression) {
		resetAccumulators();
		ListIterator<String[]> tupleIterator = tableData.listIterator();
		String columnName = null;
		Map<String, String> itemsMap = new HashMap<>();
		List<String> printedList = new ArrayList<>();
		
		List<String[]> subSelectTuples = new ArrayList<>();
		
		/* Iterate through all the tuples of the result and print them.
		 */
        while(tupleIterator.hasNext()){
        	expressionVisitor.setAccumulatorBoolean(true);
        	printPipe = false;
        	expressionVisitor.setTuple(tupleIterator.next());
        	boolean whereEvaluatesTo = true;
        	
        	if(whereExpression != null) {
        		whereExpression.accept(expressionVisitor);
        		whereEvaluatesTo = expressionVisitor.isAccumulatorBoolean();
        	}

			String[] subSelectTuple = new String[selectItemExpressionList.size()];
			int subSelectTupleIndex = 0;
			TableSchema subSelectTableSchema = new TableSchema(subSelectTableName);
			List<ColumnDefinition> subSelectColumndefinitionList = new ArrayList<>();

			if(whereEvaluatesTo) {
				
	    		for(SelectExpressionItem item : selectItemExpressionList) {
					if(item.getExpression() instanceof Function) { // For Aggregate Functions like COUNT, SUM etc..
						expressionVisitor.setFunction(); // Set a variable which tells that we are calculating a function.
					}
					if(expressionVisitor.isFunction()) {
	    				if(item.getExpression() instanceof Function) {
	    					item.accept(new QueryParser());
	    				} else {
	    					String value = "";
		        			item.accept(new QueryParser());
		        			int accumulatorType = expressionVisitor.getAccumulatorType();
		    				if(accumulatorType == 1) {
		    					value = expressionVisitor.getAccumulatorLong()+"";
			    			} else if (accumulatorType == 2) {
		    					value = expressionVisitor.getAccumulatorString();
			    			} else if (accumulatorType == 3) {
			    				String newString = new SimpleDateFormat("yyyy-MM-dd").format(expressionVisitor.getAccumulatorDate());
		    					value = newString;
			    			} else if (accumulatorType == 4) {
		    					value = (expressionVisitor.getAccumulatorDouble()+"");
			    			} 
		    				if(itemsMap.get(item.getExpression().toString()) == null) {
		    					itemsMap.put(item.getExpression().toString(), value);
		    				}
	    				}
	    			} else {
	        			item.accept(new QueryParser());
	        			int accumulatorType = expressionVisitor.getAccumulatorType();
	    				if(accumulatorType == 1) {
	    					if(QueryParser.evaluatingSubSelect) {
	    						subSelectTuple[subSelectTupleIndex] =  expressionVisitor.getAccumulatorLong()+"";
	    						subSelectTupleIndex++;
	    						ColumnDefinition subSelectColumnDefinition = new ColumnDefinition();
	    						ColDataType colDataType = new ColDataType();
	    						colDataType.setDataType("int");
	    						
	    						String subSelectColumnName = item.getAlias();
	    						if(subSelectColumnName == null) {
	    							subSelectColumnName = item.getExpression().toString();
	    						}
	    						
	    						subSelectColumnDefinition.setColDataType(colDataType);
	    						subSelectColumnDefinition.setColumnName(subSelectColumnName);
	    						
	    						subSelectColumndefinitionList.add(subSelectColumnDefinition);
	    					} else {
	    						printColumnValue(expressionVisitor.getAccumulatorLong()+"");
	    					}
		    			} else if (accumulatorType == 2) {
	    					if(QueryParser.evaluatingSubSelect) {
	    						subSelectTuple[subSelectTupleIndex] =  expressionVisitor.getAccumulatorString();
	    						subSelectTupleIndex++;
	    						
	    						ColumnDefinition subSelectColumnDefinition = new ColumnDefinition();
	    						ColDataType colDataType = new ColDataType();
	    						colDataType.setDataType("String");
	    						
	    						String subSelectColumnName = item.getAlias();
	    						if(subSelectColumnName == null) {
	    							subSelectColumnName = item.getExpression().toString();
	    						}
	    						
	    						subSelectColumnDefinition.setColDataType(colDataType);
	    						subSelectColumnDefinition.setColumnName(subSelectColumnName);
	    						
	    						subSelectColumndefinitionList.add(subSelectColumnDefinition);

	    					} else {
	    						printColumnValue(expressionVisitor.getAccumulatorString());
	    					}
		    			} else if (accumulatorType == 3) {
		    				String newString = new SimpleDateFormat("yyyy-MM-dd").format(expressionVisitor.getAccumulatorDate());
	    					if(QueryParser.evaluatingSubSelect) {
	    						subSelectTuple[subSelectTupleIndex] =  newString;
	    						subSelectTupleIndex++;

	    						ColumnDefinition subSelectColumnDefinition = new ColumnDefinition();
	    						ColDataType colDataType = new ColDataType();
	    						colDataType.setDataType("date");
	    						
	    						String subSelectColumnName = item.getAlias();
	    						if(subSelectColumnName == null) {
	    							subSelectColumnName = item.getExpression().toString();
	    						}
	    						
	    						subSelectColumnDefinition.setColDataType(colDataType);
	    						subSelectColumnDefinition.setColumnName(subSelectColumnName);
	    						
	    						subSelectColumndefinitionList.add(subSelectColumnDefinition);

	    						
	    					} else {
	    						printColumnValue(newString);
	    					}
		    			} else if (accumulatorType == 4) {
	    					if(QueryParser.evaluatingSubSelect) {
	    						subSelectTuple[subSelectTupleIndex] =  (expressionVisitor.getAccumulatorDouble()+"");
	    						subSelectTupleIndex++;
	    						
	    						ColumnDefinition subSelectColumnDefinition = new ColumnDefinition();
	    						ColDataType colDataType = new ColDataType();
	    						colDataType.setDataType("decimal");
	    						
	    						String subSelectColumnName = item.getAlias();
	    						if(subSelectColumnName == null) {
	    							subSelectColumnName = item.getExpression().toString();
	    						}
	    						
	    						subSelectColumnDefinition.setColDataType(colDataType);
	    						subSelectColumnDefinition.setColumnName(subSelectColumnName);
	    						
	    						subSelectColumndefinitionList.add(subSelectColumnDefinition);

	    						
	    					} else {
	    						printColumnValue(expressionVisitor.getAccumulatorDouble()+"");
	    					}
		    			}
        			}
    			}
    		}
			subSelectTuples.add(subSelectTuple);
			subSelectTupleIndex = 0;
			
			if(QueryParser.evaluatingSubSelect) {
				subSelectTableSchema.setcolumnDefinitions(subSelectColumndefinitionList);
				TableMetaDataService tableMetaDataSertice = TableMetaDataService.createInstance();
				tableMetaDataSertice.setTableSchema(subSelectTableName, subSelectTableSchema);
			}
			
    		 // Print newline only if we dont have a function, since function value is printed in next loop
    		if(!expressionVisitor.isFunction() && whereEvaluatesTo && !QueryParser.evaluatingSubSelect) { 
    			System.out.println();
    		}
        }
        // For printing all aggregate functions
        for(SelectExpressionItem item : selectItemExpressionList) {
			if(item.getExpression() instanceof Function) { // For Aggregate Functions like COUNT, SUM etc..
		        if(expressionVisitor.isFunction()) {
		        	if(columnPrinted)
		        		printPipe = true;
		        	Function function = (Function)item.getExpression();
					if(function.isAllColumns()) { // Means COUNT(*)
						columnName = "*";
					} else {
						columnName = ((Function)item.getExpression()).getParameters().getExpressions().get(0).toString();
					}
					if(function.getName().equalsIgnoreCase("COUNT")) {
						printColumnValue(expressionVisitor.getCountMap().get(columnName).intValue()+"");
					} else if(function.getName().equalsIgnoreCase("SUM")) {
						if(expressionVisitor.getAccumulatorType()==1) {
							printColumnValue(((Double)(expressionVisitor.getSumMap().get(columnName)[0]))+"");
						} else {
							printColumnValue(expressionVisitor.getSumMap().get(columnName)[0]+"");
						}
					} else if(function.getName().equalsIgnoreCase("AVG")) {
						printColumnValue(new Double(expressionVisitor.getSumMap().get(columnName+"_AVG")[0])/(tableData.size())+"");
					} else if(function.getName().equalsIgnoreCase("MIN")) {
						printColumnValue(expressionVisitor.getMinMap().get(columnName)+"");
					} else if(function.getName().equalsIgnoreCase("MAX")) {
						printColumnValue(expressionVisitor.getMaxMap().get(columnName)+"");
					}
		        }
			} else {
	        	if(itemsMap.get(item.getExpression().toString()) != null && !QueryParser.evaluatingSubSelect) {
	        		if(printedList.indexOf(item.getExpression().toString()) != -1) {
	        			printColumnValue(itemsMap.get(item.getExpression().toString()));
	        		}
	        	}
	        }
        }
        System.out.println();
        
        if(QueryParser.evaluatingSubSelect) {
        	tableDataServiceMap.put(Helper.subSelectTableName, new TableDataService(Helper.subSelectTableName, subSelectTuples));
        	QueryParser.evaluatingSubSelect = false;
        }
	}

	
	
	private static String getDataType(Function function, Map<String, ColumnDefinition> originalColumnsDefinitionMap) {
/*		List expressionList = function.getParameters().getExpressions();
		boolean isDecimal;
		for(Object expression : expressionList) {
			if(expression instanceof BinaryExpression) {
				String dataType = getDataType((BinaryExpression)expression, originalColumnsDefinitionMap);
				if(dataType.equalsIgnoreCase("Decimal")){
					return "Decimal";
				}
			} else {
				if(originalColumnsDefinitionMap.get(((Column)expression).getColumnName()).getColDataType().getDataType().equalsIgnoreCase("Decimal")) {
					return "Decimal";
				}
			}
		}
		return "Long";
*/
		return "Decimal";
		}
		
	
/*	private static List<String[]> getTableDataForJoins(List<String> tablesNameList) {
		List<String[]> productTableListData;
		List<List<String[]>> tableDataList = new ArrayList<>();
		for(int index=tablesNameList.size()-1; index>=0;index-- ) {
			boolean completed = allTablesComplete(tablesNameList);
			if(completed) {
				return null;
			}
			String tableName = tablesNameList.get(index);
			TableDataService tableDataService =  tableDataServiceMap.get(tableName);
			String nextTableName = null;
			if(index != tablesNameList.size()-1) {
				nextTableName = tablesNameList.get(index+1);
			}
			if(nextTableName != null) { // all but last table Of Joins
				boolean innerCompleted = allTablesComplete(tablesNameList.subList(tablesNameList.indexOf(nextTableName), tablesNameList.size()));
				if(innerCompleted && !tableDataService.isReadComplete()) {
					TableDataService.resetAllInnerScanners(tablesNameList, nextTableName);
					tableDataService.getTableData();
				}
				if(tableDataService.getTableDataFromMemory() == null) {
					tableDataService.getTableData();
				}
			} else { // Last Table Of Joins
				if(tableDataService.isReadComplete()) {
					tableDataService.resetTableScanner();
					tableDataService.setReadComplete(false);
				}
				tableDataService.getTableData();
				//TableDataService.resetAllInnerScanners(tablesNameList, tableDataService.getTableName());
			}
		}
		for(String tableName : tablesNameList) {
			TableDataService tableDataService =  tableDataServiceMap.get(tableName);
			List<String[]> tableData = tableDataService.getTableDataFromMemory();
			tableDataList.add(tableData);
		}
		
		ListIterator<List<String[]>> tableDataListIterator = tableDataList.listIterator();
		productTableListData = tableDataListIterator.next();

		while(tableDataListIterator.hasNext()) {
			List<String[]> tableDataRight = tableDataListIterator.next();
			productTableListData = getCartesianProduct(productTableListData, tableDataRight);
		}
		
		return productTableListData;
	}
*/	
/*	private static boolean allTablesComplete(List<String>tablesNameList) {
		for(String tableName : tablesNameList) {
			if(!tableDataServiceMap.get(tableName).isReadComplete()) {
				return false;
			}
		}
		return true;
	}
*/
	private static void printColumnValue(String value) {
		PrintStream outStream = System.out;
		PrintWriter printWriter = new PrintWriter(outStream, true);
		try{
			if(Helper.isWriteToFile()) {
				printWriter = new PrintWriter(new FileWriter(SWAP_DIRECTORY + File.separatorChar + Helper.ORDER_BY_VIEW_IDENTIFIER, true), true);
			}
			if(!printPipe) {
				printPipe = true;
				columnPrinted = true;
			} else {
					printWriter.write("|");
			}
			printWriter.write(value);
			printWriter.flush();
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println(e.getMessage());
		} finally {
			if(Helper.isWriteToFile()) {
				printWriter.close();
			}
		}
	}
	
	public static void resetAccumulators() {
		expressionVisitor.setAccumulatorDouble(0d);
		expressionVisitor.setAccumulatorLong(0);
		expressionVisitor.setAccumulatorString("");
		expressionVisitor.setAccumulatorDate(new Date());
		expressionVisitor.setAccumulatorType(-1);
		expressionVisitor.setCountMap(new HashMap<String, Long>());
		expressionVisitor.setSumMap(new HashMap<String, Double[]>());
		expressionVisitor.setMaxMap(new HashMap<String, Double>());
		expressionVisitor.setMinMap(new HashMap<String, Double>());
		expressionVisitor.expressionAlias = null;
		expressionVisitor.setFunction(false);
	}
	
	public static List<String[]> getCartesianProduct(List<String[]> tableDataLeft, List<String[]> tableDataRight) {
		List<String[]> cartesianProduct = new ArrayList<>();
		for(int leftIndex = 0; leftIndex<tableDataLeft.size(); leftIndex++) {
			String[] leftTuple = tableDataLeft.get(leftIndex);
			for(int rightIndex = 0; rightIndex<tableDataRight.size(); rightIndex++) {
				String[] rightTuple = tableDataRight.get(rightIndex);
				String[] productTuple = new String[leftTuple.length+ rightTuple.length];
				System.arraycopy(leftTuple, 0, productTuple, 0, leftTuple.length);
				System.arraycopy(rightTuple, 0, productTuple, leftTuple.length, rightTuple.length);
				cartesianProduct.add(productTuple);
			}
		}
		return cartesianProduct;
	}
	}
