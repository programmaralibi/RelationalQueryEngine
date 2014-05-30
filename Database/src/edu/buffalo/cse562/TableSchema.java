package edu.buffalo.cse562;

import java.util.List;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class TableSchema {
	private String tableName;
	private List<ColumnDefinition> columnDefinitions;
	
	public TableSchema(String name, List<ColumnDefinition> column) {
		this.tableName = name;
		this.columnDefinitions = column;
	}
	
	public TableSchema(String name) {
		this.tableName = name;
	}
	
	public String getTableName() {
		return tableName;
	}

	public void setName(String name) {
		this.tableName = name;
	}

	public List<ColumnDefinition> getColumnDefinitions() {
		return columnDefinitions;
	}

	public void setcolumnDefinitions(List<ColumnDefinition> columnDefinitions) {
		this.columnDefinitions = columnDefinitions;
	}
	
	public String toString() {
		return this.tableName;
	}

}
