
package edu.buffalo.cse562;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import edu.buffalo.cse562.queryParser.QueryParser;

public class GroupByHelper {
	
	public static Map<String, TableDataService> tableDataServiceMap = Helper.tableDataServiceMap; 
	private static TableDataModelling expressionVisitor = Helper.getExpressionVisitor();  //Table Data Modelling implements Expressionvisitor
	private static boolean printPipe = false;
	private static boolean columnPrinted = false;
	private static String newTableName = "GROUPBY_VIEW";
	public static final String subSelectTableName = "SUBSELECT_TABLE";
	private static HashMap<String, String[]> groupsMap = new HashMap<>();   // Map with Keys : grouping Parameters & Value : Group where Group is List of Tuples
	public static List<String> joinTablesList;
	public static final String PRODUCT_TABLE_IDENTIFIER = "PRODUCT_";
	private static String SWAP_DIRECTORY = Main.SWAP_DIRECTORY;
	
	
	public static void printResultsAfterGroup(PlainSelect plainSelect , List<String[]> tableData, TableSchema tableSchema) {
		List<SelectExpressionItem> selectItemExpressionList = plainSelect.getSelectItems();
		for(String[] tuple: tableData) {
			if(tuple[0]!= null) {
				boolean firstTuple = true;
				for(SelectExpressionItem item : selectItemExpressionList) {
					String columnName = item.getAlias();
					if(columnName == null) {
						columnName = item.toString();
					}
					List<String> columnNames = Helper.getColumnNames(tableSchema);
					int columnIndex = columnNames.indexOf(columnName);
					if(firstTuple) {
						System.out.print(tuple[columnIndex]);
						firstTuple = false;
					} else {
						System.out.print("|"+tuple[columnIndex]);
					}
				}
				System.out.println();
			}
		}
	}
	
	
/*	private static List<String[]> processTableDataGroups(List<SelectExpressionItem>  selectItemExpressionList, List<String[]> group, Expression whereExpression, TableSchema tableSchema) {
		Helper.resetAccumulators();
		ListIterator<String[]> tupleIterator = group.listIterator();
		String columnName = null;
		Map<String, String> itemsMap = new HashMap<>();
		

		
		 Iterate through all the tuples of the result and print them.
		 
		
		List<String[]> tupleList = new ArrayList<>();
		String newTuple[] = new String[selectItemExpressionList.size()];
		int index = 0;
		
		String updatedSchemaName = "GROUPBY_"+tableSchema.getTableName();
		
		Map<String, ColumnDefinition> originalColumnsDefinitionMap = new HashMap<>();
		for(ColumnDefinition columnDefintion : tableSchema.getcolumnDefinitions()) {
			originalColumnsDefinitionMap.put(columnDefintion.getColumnName(), columnDefintion);
		}
		
		int actualGroupsize = 0; 
        while(tupleIterator.hasNext()){
        	expressionVisitor.setAccumulatorBoolean(true);
        	printPipe = false;
        	expressionVisitor.setTuple(tupleIterator.next());
        	boolean whereEvaluatesTo = true;
        	
        	if(whereExpression != null) {
        		whereExpression.accept(expressionVisitor);
        		whereEvaluatesTo = expressionVisitor.isAccumulatorBoolean();
        	}
			if(whereEvaluatesTo) {
				actualGroupsize ++;
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
    					String value = "";
	        			
	    				if(accumulatorType == 1) {
	    					value = (expressionVisitor.getAccumulatorLong()+"");
		    			} else if (accumulatorType == 2) {
		    				value = (expressionVisitor.getAccumulatorString());
		    			} else if (accumulatorType == 3) {
		    				String newString = new SimpleDateFormat("yyyy-MM-dd").format(expressionVisitor.getAccumulatorDate());
		    				value = newString;
		    			} else if (accumulatorType == 4) {
		    				value = expressionVisitor.getAccumulatorDouble()+"";
		    			}
	    				
	    				if(itemsMap.get(item.getExpression().toString()) == null) {
	    					itemsMap.put(item.getExpression().toString(), value);
	    				}
        			}
    			}
    		}
    		 // Print newline only if we dont have a function, since function value is printed in next loop
    		if(!expressionVisitor.isFunction() && whereEvaluatesTo) { 
    			//System.out.println();
    			tupleList.add(newTuple);
    			newTuple = new String[selectItemExpressionList.size()];
    			index=0;
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
						newTuple[index] = expressionVisitor.getCountMap().get(columnName).intValue()+"";
						index++;
					} else if(function.getName().equalsIgnoreCase("SUM")) {
						if(expressionVisitor.getAccumulatorType()==1) {
							newTuple[index] = ((Double)(expressionVisitor.getSumMap().get(columnName))[0]).longValue()+"";
							index++;
						} else {
							newTuple[index] = (expressionVisitor.getSumMap().get(columnName)+"");
							index++;
						}
					} else if(function.getName().equalsIgnoreCase("AVG")) {
						newTuple[index] = (new Double(expressionVisitor.getSumMap().get(columnName+"_AVG")[0])/expressionVisitor.getSumMap().get(columnName+"_AVG")[1])+"";
						index++;
					} else if(function.getName().equalsIgnoreCase("MIN")) {
						newTuple[index] = (expressionVisitor.getMinMap().get(columnName)+"");
						index++;
					} else if(function.getName().equalsIgnoreCase("MAX")) {
						newTuple[index] = (expressionVisitor.getMaxMap().get(columnName)+"");
						index++;
					}
		        }
			}  else {
	        	if(itemsMap.get(item.getExpression().toString()) != null) {
	        		newTuple[index] = (itemsMap.get(item.getExpression().toString()));
	        		index++;
	        	}
	        }
        }
        if(newTuple[0] != null) {
        	tupleList.add(newTuple);
        	newTuple = new String[selectItemExpressionList.size()];
        	index=0;
        }
		return tupleList;
	}
	
*/	
/*	
	private static List<List<String[]>> createTableDataGroups(List<String[]> tableData, TableSchema tableSchema, List<Expression> groupByExpressionList) {

		List<String> columnNames = Helper.getColumnNames(tableSchema);
		Iterator<Expression> groupByExpressionIterator;
		HashMap<String, List<String[]>> groupsMap = new HashMap<>();   // Map with Keys : grouping Parameters & Value : Group where Group is List of Tuples
		List<String> groupList = new ArrayList<>();
		
		for(String[] tuple : tableData) {
			groupByExpressionIterator = groupByExpressionList.iterator();
			StringBuffer parameterValueList = new StringBuffer("");

			while(groupByExpressionIterator.hasNext()) {
				String currentGroupingParameter = groupByExpressionIterator.next().toString();
				int indexOfGroupingParam = columnNames.indexOf(currentGroupingParameter);
				parameterValueList.append(tuple[indexOfGroupingParam]);
			}

			String key = parameterValueList.toString();
			if(groupsMap.get(key) == null)  {
				groupList.add(key);
				groupList.get(0);
				List<String[]> group = new ArrayList<String[]>();
				group.add(tuple);
				groupsMap.put(key, group);
			} else {
				List<String[]> group = groupsMap.get(key);
				group.add(tuple);
				groupsMap.put(key, group);
			}
		}

		List<List<String[]>> tableDataGroups = new ArrayList<>();
		for(String key : groupList) {
			tableDataGroups.add(groupsMap.get(key));
		}
		return tableDataGroups;
	}
*/
	// Latest function for solving the groups. 
	static void processGroups(PlainSelect plainSelect, List<SelectExpressionItem> selectItemExpressionList, String tableName, Expression whereExpression, List<String[]> tableData) {
		List<Expression> groupByExpressionList = plainSelect.getGroupByColumnReferences();
		if(!tableName.contains(subSelectTableName) && !tableName.startsWith(PRODUCT_TABLE_IDENTIFIER)) {
			//Helper.getTableDataService(tableName).resetTableScanner(tableName);
		}
		createGroups(tableName, tableData, selectItemExpressionList, groupByExpressionList, whereExpression);
		fillvaluesInGroups(selectItemExpressionList); // Fill values of aggregate Functions from Different Maps
	}

	
	private static void fillvaluesInGroups(List<SelectExpressionItem> selectItemExpressionList) {
		Set<String> keySet = groupsMap.keySet();
		String columnName = null;
		for(String key : keySet) { // Looping on all the groups
			int columnIndex = 0;
    		for(SelectExpressionItem item : selectItemExpressionList) {
				if(item.getExpression() instanceof Function) {
		        	Function function = (Function)item.getExpression();
					if(function.isAllColumns()) { // Means COUNT(*)
						columnName = "*";
					} else {
						columnName = ((Function)item.getExpression()).getParameters().getExpressions().get(0).toString();
					}
					
					String identifier = columnName;
					if(item.getAlias() != null) {
						identifier = item.getAlias();
					}
					identifier = key+"_"+identifier;
					
					if(function.getName().equalsIgnoreCase("COUNT")) {
						String[] oldTuple = groupsMap.get(key);
						oldTuple[columnIndex] = expressionVisitor.getCountMap().get(identifier)+""; 
						groupsMap.put(key, oldTuple); // Now the function values have been updated
 					} else if(function.getName().equalsIgnoreCase("SUM")) {
						String[] oldTuple = groupsMap.get(key);
						Double value = expressionVisitor.getSumMap().get(identifier)[0]; 
						if(expressionVisitor.getAccumulatorType()==1) { // Means Data type is long
							oldTuple[columnIndex] = value.longValue()+"";
						} else { // the value is of type double
							oldTuple[columnIndex] = value+"";
						}
						groupsMap.put(key, oldTuple); // Now the function values have been updated
					} else if(function.getName().equalsIgnoreCase("AVG")) {
						String[] oldTuple = groupsMap.get(key);
						oldTuple[columnIndex] = (new Double(expressionVisitor.getSumMap().get(columnName+"_AVG")[0])/new Double(expressionVisitor.getSumMap().get(columnName+"_AVG")[1]))+"";
						groupsMap.put(key, oldTuple); // Now the function values have been updated

					} else if(function.getName().equalsIgnoreCase("MIN")) {
/*						newTuple[index] = (expressionVisitor.getMinMap().get(columnName)+"");
						index++;
*/					} else if(function.getName().equalsIgnoreCase("MAX")) {
/*						newTuple[index] = (expressionVisitor.getMaxMap().get(columnName)+"");
						index++;
*/					}

				} 
				columnIndex++;
    		}
		}
	}
	
	private static void createGroups(String tableName, List<String[]> tableData, List<SelectExpressionItem> selectItemExpressionList, List<Expression> groupByExpressionList, Expression whereExpression) {
		ListIterator<String[]> tupleIterator = tableData.listIterator();
		Iterator<Expression> groupByExpressionIterator;
		TableMetaDataService tableMetaDataService = TableMetaDataService.createInstance();
		TableSchema tableSchema = tableMetaDataService.getTableSchema(tableName); 
		List<String> columnNames = Helper.getColumnNames(tableSchema);

		/* Iterate through all the tuples of the result and print them.
		 */
		
		expressionVisitor.setUpdatedTableName(tableName);

        while(tupleIterator.hasNext()){
        	
        	String columnName= null;
        	String[] tuple = tupleIterator.next();
        	expressionVisitor.setAccumulatorBoolean(true);
        	printPipe = false;
        	expressionVisitor.setTuple(tuple);
        	
        	boolean whereEvaluatesTo = true;
        	if(whereExpression != null) {
        		whereExpression.accept(expressionVisitor);
        		whereEvaluatesTo = expressionVisitor.isAccumulatorBoolean();
        	}

			if(whereEvaluatesTo) {

				groupByExpressionIterator = groupByExpressionList.iterator();
				StringBuffer parameterValueList = new StringBuffer("");

				while(groupByExpressionIterator.hasNext()) {
					String currentGroupingParameter = groupByExpressionIterator.next().toString();
					if(currentGroupingParameter.indexOf(".") != -1) {
						String parameterTableName = currentGroupingParameter.split("\\.")[0].toUpperCase();
						String parameterColumnName = currentGroupingParameter.split("\\.")[1];
						currentGroupingParameter = parameterTableName + "." + parameterColumnName;
					}
					int indexOfGroupingParam = columnNames.indexOf(currentGroupingParameter);
					parameterValueList.append(tuple[indexOfGroupingParam]);
				}

				String key = parameterValueList.toString();
				String[] newTuple = new String[selectItemExpressionList.size()];
				int newTupleIndex = 0;
	    		for(SelectExpressionItem item : selectItemExpressionList) {
	    			
    				if(item.getExpression() instanceof Function) {
    					newTupleIndex++; // So that the column for function value is null
    		        	Function function = (Function)item.getExpression();
    					if(function.isAllColumns()) { // Means COUNT(*)
    						columnName = "*";
    					} else {
    						columnName = ((Function)item.getExpression()).getParameters().getExpressions().get(0).toString();
    					}
						String partialIdentifier = columnName;
						//String identifier = columnName;
						
						if(item.getAlias() != null) {
							partialIdentifier = item.getAlias();
						}

						//******************//
						String identifier = key + "_" + partialIdentifier;
						TableDataModelling.expressionAlias = identifier;

    					item.accept(new QueryParser());
    /*					TableDataModelling.expressionAlias=null;

    					if(function.getName().equalsIgnoreCase("COUNT")) {
    						Long value = expressionVisitor.getCountMap().get(identifier); // Get the old value for the count
    						if(value == null){
    							expressionVisitor.getCountMap().put(identifier, 1l);
    						} else {
    							expressionVisitor.getCountMap().put(identifier, value+1);
    						}
    					} else if(function.getName().equalsIgnoreCase("SUM")) {
    						Double value = expressionVisitor.getSumMap().get(identifier)[0];
    						if(value == null){
    							expressionVisitor.getSumMap().put(identifier, new Double[]{0d,0d});
    						} 
    						if(expressionVisitor.getAccumulatorType()==1) { // Means Data type is long
    							Double oldValue = expressionVisitor.getSumMap().get(identifier)[0];
    							Double oldSize = expressionVisitor.getSumMap().get(identifier)[1];
    							Double newValue = oldValue+expressionVisitor.getAccumulatorLong();
    							Double newSize = oldSize + 1;

    							expressionVisitor.getSumMap().put(identifier, new Double[]{newValue, newSize});
    						} else { // the value is of type double
    							
    							Double oldValue = expressionVisitor.getSumMap().get(identifier)[0];
    							Double oldSize = expressionVisitor.getSumMap().get(identifier)[1];
    							Double newValue = oldValue+expressionVisitor.getAccumulatorDouble();
    							Double newSize = oldSize + 1;

    							expressionVisitor.getSumMap().put(identifier, new Double[]{newValue, newSize});
    						}
    					} else if(function.getName().equalsIgnoreCase("AVG")) {
    						String[] oldTuple = groupsMap.get(key);
    						//oldTuple[columnIndex] = (new Double(expressionVisitor.getSumMap().get(columnName+"_AVG"))/(actualGroupsize))+"";
    						groupsMap.put(key, oldTuple); // Now the function values have been updated

    					} else if(function.getName().equalsIgnoreCase("MIN")) {
    						newTuple[index] = (expressionVisitor.getMinMap().get(columnName)+"");
    						index++;
    					} else if(function.getName().equalsIgnoreCase("MAX")) {
    						newTuple[index] = (expressionVisitor.getMaxMap().get(columnName)+"");
    						index++;
    					}
*/
    				}else {
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
	    				newTuple[newTupleIndex] = value;
	    				newTupleIndex++;
    				}
	    		}
	    		if(groupsMap.get(key) == null) {
	    			groupsMap.put(key, newTuple);
	    		}
	    	}  // End of whereEvaluatesTo
		} 
	}
	
	public static void printGroups() {
		Map localGroupsMap = groupsMap;
		Iterator<String> groupIterator = localGroupsMap.keySet().iterator();
		PrintStream outStream = System.out;
		PrintWriter printWriter = new PrintWriter(outStream);
		try {
			if(Helper.isWriteToFile()) {
				printWriter = new PrintWriter(new FileWriter(SWAP_DIRECTORY + File.separatorChar + Helper.ORDER_BY_VIEW_IDENTIFIER, true), true);
			}
			while(groupIterator.hasNext()) {
				String[] group = (String[])localGroupsMap.get(groupIterator.next());
				for(String column : group) {
					printWriter.write(column+"|");
				}
				printWriter.write("\n");
				printWriter.flush();
			}
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
	

	static String processJoins(List<Join> joins, FromItem fromItem, Expression whereExpression) {
		StringBuffer productTableName = new StringBuffer("PRODUCT");
		List<String> tableNamesList = new ArrayList<>();
		String leftTableName = ((Table)fromItem).getName().toUpperCase();
		tableNamesList.add(leftTableName);
		productTableName.append("_"+leftTableName);

		for (Join join : joins) {
			if(join.getRightItem() instanceof Table) {
				join.getRightItem().accept(new QueryParser());  // Loading data
				String rightTableName = ((Table)join.getRightItem()).getName().toUpperCase();
				productTableName.append("_"+rightTableName);
				tableNamesList.add(rightTableName);
			}
		}

		joinTablesList = tableNamesList;
		List<String[]> tableDataForJoins = getTableDataForJoins(tableNamesList, whereExpression);
		TableDataService tableDataService = new TableDataService(productTableName.toString(), tableDataForJoins);
		tableDataServiceMap.put(productTableName.toString(), tableDataService);
		expressionVisitor.setTableDataService(tableDataService);
		return productTableName.toString();
	}
	
	private static List<String[]> getTableDataForJoins(List<String> tablesNameList, Expression whereExpression) {
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
			productTableListData = Helper.getCartesianProduct(productTableListData, tableDataRight);
		}
		
		return productTableListData;
	}
	
	private static boolean allTablesComplete(List<String>tablesNameList) {
		for(String tableName : tablesNameList) {
			if(!tableDataServiceMap.get(tableName).isReadComplete()) {
				return false;
			}
		}
		return true;
	}
	
}
