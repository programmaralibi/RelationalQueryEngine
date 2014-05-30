package edu.buffalo.cse562;

import java.util.HashMap;
import java.util.Map;

public class TableMetaDataService {
	private Map<String, TableSchema> allTableSchema = new HashMap<String, TableSchema>();
    private static TableMetaDataService tableMetaDataService = null;
    
	private TableMetaDataService() {
		
	}
	
	public TableSchema getTableSchema(String tableName) {
		return allTableSchema.get(tableName.toUpperCase());
	}

	public void setTableSchema(String tableName, TableSchema tableSchema) {
		allTableSchema.put(tableName.toUpperCase(), tableSchema);
	}
		
	public Map<String, TableSchema> getAllTableSchema() {
		return allTableSchema;
	}

	public void setAllTableSchema(Map<String, TableSchema> allTableSchema) {
		this.allTableSchema = allTableSchema;
	}
	
	public static TableMetaDataService createInstance() {
		if(tableMetaDataService == null) {
			tableMetaDataService = new TableMetaDataService();
		}
		return tableMetaDataService;
	}

}
