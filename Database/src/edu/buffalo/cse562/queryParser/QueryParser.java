package edu.buffalo.cse562.queryParser;

import java.util.Iterator;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import edu.buffalo.cse562.Helper;
import edu.buffalo.cse562.TableDataModelling;
import edu.buffalo.cse562.TableDataService;
import edu.buffalo.cse562.TableMetaDataService;
import edu.buffalo.cse562.TableSchema;



public class QueryParser implements SelectVisitor, FromItemVisitor, ItemsListVisitor, StatementVisitor, SelectItemVisitor {

	private TableMetaDataService tableMetaDataService = TableMetaDataService.createInstance();
	public static boolean evaluatingSubSelect = false;
	
	/*
	 * A Major imitation lies in the calling of various methods like visit(CreateTable createTable) and visit(Table tableName) 
	 * The private members used by these methods, query and tableSchema are used in other methods.
	 * Concurrent calling of these methods over different queries will cause error.  
	 */
	
	private TableSchema tableSchema = null;
	
	private boolean updateExpressionVisitor = true; // It is used in joins inside Groupby where we don't want to alter  
	
	public boolean isUpdateExpressionVisitor() {
		return updateExpressionVisitor;
	}


	public void setUpdateExpressionVisitor(boolean updateExpressionVisitor) {
		this.updateExpressionVisitor = updateExpressionVisitor;
	}


	public TableSchema createTableSchema (Statement statement) {
		statement.accept(this);
		return tableSchema;
	}

	
	// Method called while parsing Select Query
	public void visit(PlainSelect plainSelect) {
		plainSelect.getFromItem().accept(this); // Data get loaded
		Helper.processQuery(plainSelect);
	}
	

	public void visit(Union union) {
		for (Iterator iter = union.getPlainSelects().iterator(); iter.hasNext();) {
			PlainSelect plainSelect = (PlainSelect) iter.next();
			visit(plainSelect);
		}
	}

	
	// Need to update the way we create objects of TableDataService and ExprerssionVisitor.
	public void visit(Table table) {
		TableDataService tableDataService = Helper.getTableDataService(table.getName());
		if(tableDataService  == null) {
			tableDataService = new TableDataService(table.getName().toUpperCase());
		}
		Helper.tableDataServiceMap.put(table.getName().toUpperCase(), tableDataService);
		if(table.getAlias()!=null){
			String aliasName = table.getAlias();
			Helper.tableDataServiceMap.put(aliasName, tableDataService);
			TableMetaDataService tableMetaDataService = TableMetaDataService.createInstance();
			tableMetaDataService.setTableSchema(aliasName, tableMetaDataService.getTableSchema(table.getName().toUpperCase()));
		}
	}

	public void visit(SubSelect subSelect) {
		evaluatingSubSelect = true;
		subSelect.getSelectBody().accept(this);
	}

	public void visit(AndExpression andExpression) {
		visitBinaryExpression(andExpression);
	}


	public void visit(Column tableColumn) {
		tableColumn.accept(Helper.getExpressionVisitor());
	}


	public void visit(DoubleValue doubleValue) {
	}


	public void visit(InverseExpression inverseExpression) {
		inverseExpression.getExpression().accept(Helper.getExpressionVisitor());
	}



	public void visit(ExistsExpression existsExpression) {
		existsExpression.getRightExpression().accept(Helper.getExpressionVisitor());
	}


	public void visit(OrExpression orExpression) {
		visitBinaryExpression(orExpression);
	}

	public void visit(Parenthesis parenthesis) {
		parenthesis.getExpression().accept(Helper.getExpressionVisitor());
	}

	public void visit(StringValue stringValue) {
	}

	public void visit(Subtraction subtraction) {
		visitBinaryExpression(subtraction);
	}

	public void visitBinaryExpression(BinaryExpression binaryExpression) {
		Expression leftExpression = binaryExpression.getLeftExpression();
		//leftExpression.accept(this);
		Expression rightExpression = binaryExpression.getRightExpression();
		leftExpression.accept(Helper.getExpressionVisitor());
	}

	public void visit(ExpressionList expressionList) {
		for (Iterator iter = expressionList.getExpressions().iterator(); iter.hasNext();) {
			Expression expression = (Expression) iter.next();
			expression.accept(Helper.getExpressionVisitor());
		}
	}

	public void visit(DateValue dateValue) {
	}
	
	public void visit(TimestampValue timestampValue) {
	}
	
	public void visit(TimeValue timeValue) {
	}



	public void visit(SubJoin subjoin) {
		subjoin.getLeft().accept(this);
		subjoin.getJoin().getRightItem().accept(this);
	}


	// The First method to be called when we parse a Select Query.
	public void visit(Select select) {
//		select.getWithItemsList();
		select.getSelectBody().accept(this);
	}

	@Override
	public void visit(Delete delete) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Update update) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Insert insert) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Replace replace) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Drop drop) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Truncate truncate) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CreateTable createTable) {
		//createTable.getTable().accept(this);
		String tableName = createTable.getTable().getWholeTableName().toUpperCase();
		tableSchema = new TableSchema(tableName, createTable.getColumnDefinitions());
		tableMetaDataService.getAllTableSchema().put(tableName, tableSchema);
	}
	
	@Override
	public void visit(AllColumns allColumns) {
		// TODO Auto-generated method stub
	}

	@Override
	public void visit(AllTableColumns allTableColumns) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SelectExpressionItem selectExpressionItem) {
/*		if(selectExpressionItem.getExpression() instanceof Function) {
			TableDataModelling.expressionAlias = selectExpressionItem.getAlias();
		}
*/		selectExpressionItem.getExpression().accept(Helper.getExpressionVisitor());
	}

}