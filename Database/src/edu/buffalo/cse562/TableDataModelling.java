package edu.buffalo.cse562;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

public class TableDataModelling implements ExpressionVisitor {

	private TableDataService tableDataService;
	private TableMetaDataService tableMetaDataService = TableMetaDataService.createInstance();
	private Object expressionAsObject;
	private int classType;
	private String[] tuple;
	private Long accumulatorLong = new Long(0); // 1
	private String accumulatorString; // 2
	private Date accumulatorDate; //3
	private boolean accumulatorBoolean = true; 
	private Double accumulatorDouble; // 4

	private int accumulatorType;
	private boolean isFunction = false;
	
	private Map<String, Long> countMap = new HashMap<>();
	private Map<String, Double[]> sumMap = new HashMap<>();
	private Map<String, Double> maxMap = new HashMap<>();
	private Map<String, Double> minMap = new HashMap<>();
	private Map<Expression, String> aliasMap = new HashMap<>();
	public static String expressionAlias = null;

	
	private TableSchema tableSchema = null;
	private String updatedTableName = null;
	private String tableName = null;
	private int columnIndex = 0;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setUpdatedTableName(String updatedTableName) {
		this.updatedTableName = updatedTableName;
	}

	public boolean isAccumulatorBoolean() {
		return accumulatorBoolean;
	}

	public void setAccumulatorBoolean(boolean accumulatorBoolean) {
		this.accumulatorBoolean = accumulatorBoolean;
	}
	
	public Double getAccumulatorDouble() {
		return accumulatorDouble;
	}

	public void setAccumulatorDouble(Double accumulatorDouble) {
		this.accumulatorDouble = accumulatorDouble;
	}

	public Map<String, Double> getMaxMap() {
		return maxMap;
	}

	public void setMaxMap(Map<String, Double> maxMap) {
		this.maxMap = maxMap;
	}

	public Map<String, Double> getMinMap() {
		return minMap;
	}

	public void setMinMap(Map<String, Double> minMap) {
		this.minMap = minMap;
	}


	public Map<String, Double[]> getSumMap() {
		return sumMap;
	}

	public void setSumMap(Map<String, Double[]> sumMap) {
		this.sumMap = sumMap;
	}

	public void setCountMap(Map<String, Long> countMap) {
		this.countMap = countMap;
	}
	
	public void setAliasMap(Map<Expression, String> aliasMap) {
		this.aliasMap= aliasMap;
	}

	public Map<Expression, String> getAliasMap() {
		return aliasMap;
	}

	
	public void setTableDataService(TableDataService tableDataService) {
		this.tableDataService = tableDataService;
	}

	public void setTableMetaDataService(TableMetaDataService tableMetaDataService) {
		this.tableMetaDataService = tableMetaDataService;
	}

	public void setExpressionAsObject(Object expressionAsObject) {
		this.expressionAsObject = expressionAsObject;
	}

	public void setClassType(int classType) {
		this.classType = classType;
	}

	public void setAccumulatorLong(Long accumulatorLong) {
		this.accumulatorLong = accumulatorLong;
	}

	public void setAccumulatorString(String accumulatorString) {
		this.accumulatorString = accumulatorString;
	}

	public void setAccumulatorDate(Date accumulatorDate) {
		this.accumulatorDate = accumulatorDate;
	}

	public void setAccumulatorType(int accumulatorType) {
		this.accumulatorType = accumulatorType;
	}

	
	public void setFunction(boolean isFunction) {
		this.isFunction = isFunction;
	}

	public boolean isFunction() {
		return this.isFunction;
	}

	public void setFunction() {
		this.isFunction = true;
	}

	public void setTuple(String[] tuple) {
		this.tuple = tuple;
	}
	
	public int getAccumulatorType() {
		return accumulatorType;
	}

	public long getAccumulatorLong() {
		return accumulatorLong;
	}
	
	public void setAccumulatorLong(long val) {
		this.accumulatorLong = new Long(val);
	}

	public String getAccumulatorString() {
		return accumulatorString;
	}
	public Date getAccumulatorDate() {
		return accumulatorDate;
	}

	public Map<String, Long> getCountMap() {
		return this.countMap;
	}

	
	@Override
	public void visit(NullValue nullValue) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Function function) {
		String functionName = function.getName();
		String columnName = null;
		
		if(functionName.equalsIgnoreCase("DATE")) {
 			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
 			try {
 				String date = function.getParameters().getExpressions().iterator().next().toString().replace("'", "");
				accumulatorDate = dateFormat.parse(date.toString());
				accumulatorType = 3;
 			} catch (ParseException e) {
				System.out.println("Illegal Date format. Require yyyy-MM-dd. "+e.getMessage());
			}
 			return;
		} else if(functionName.equalsIgnoreCase("MULTIPLICATION")){
				Iterator<Expression> iterator = function.getParameters().getExpressions().iterator();
				while(iterator.hasNext()) {
					System.out.println(iterator.next());
				}
				return;
		}
	
		if(function.isAllColumns()) {
		} else {
			Iterator<Expression> expressionIterator = function.getParameters().getExpressions().iterator();
			Expression expression = expressionIterator.next();
			expression.accept(this);
			columnName = expression.toString();
		}
		
		if(TableDataModelling.expressionAlias != null){
			columnName = TableDataModelling.expressionAlias;
		}

		if(functionName.equalsIgnoreCase("COUNT")) {
			if(function.isAllColumns()) {
				if(countMap.get("*") == null) {
					countMap.put("*", 1l);
				} else {
					countMap.put("*", countMap.get("*")+1l);
				}
			} else {
				if(countMap.get(columnName) == null) {
					countMap.put(columnName, 0l);
				}
				if(accumulatorType==1 && accumulatorLong!=null) {
					countMap.put(columnName, countMap.get(columnName) + 1);
				} else if(accumulatorType==2 && accumulatorString!=null && !accumulatorString.equals("")) {
					countMap.put(columnName, countMap.get(columnName) + 1);
				} else if(accumulatorType==3 && accumulatorDate!=null) {
					countMap.put(columnName, countMap.get(columnName) + 1);
				}
			}
		} else if(functionName.equalsIgnoreCase("SUM") || functionName.equalsIgnoreCase("AVG")) {
			if(functionName.equalsIgnoreCase("AVG")) {
				columnName += "_AVG";
			}
			if(sumMap.get(columnName) == null) {
				sumMap.put(columnName, new Double[]{0d ,0d});
			}
			
			if(accumulatorType == 1){
				if(accumulatorLong!=null) {
					Double oldValue = sumMap.get(columnName)[0];
					Double oldSize = sumMap.get(columnName)[1];
					Double newValue = oldValue + accumulatorLong;
					Double newSize = oldSize + 1;

					sumMap.put(columnName, new Double[]{newValue, newSize});
				}
			} else if (accumulatorType == 4) {
				if(accumulatorDouble!=null) {
					Double oldValue = sumMap.get(columnName)[0];
					Double oldSize = sumMap.get(columnName)[1];
					Double newValue = oldValue + accumulatorDouble;
					Double newSize = oldSize + 1;

					sumMap.put(columnName, new Double[]{newValue, newSize});
				}
			}
 		} else if(functionName.equalsIgnoreCase("MAX")) {
 			if(maxMap.get(columnName) == null) {
				if(accumulatorType == 1){
					if(accumulatorLong!=null) {
						maxMap.put(columnName, new Double(accumulatorLong));
					}
				} else if (accumulatorType == 4) {
					if(accumulatorDouble!=null) {
						maxMap.put(columnName, accumulatorDouble);
					}
				}
 			} else {
				if(accumulatorType == 1){
					maxMap.put(columnName, (accumulatorLong< maxMap.get(columnName)) ? maxMap.get(columnName) : accumulatorLong);
				} else if(accumulatorType == 4){
					maxMap.put(columnName, (accumulatorDouble< maxMap.get(columnName)) ? maxMap.get(columnName) : accumulatorDouble); 
				}
 			}
 		} else if(functionName.equalsIgnoreCase("MIN")) {
 			if(minMap.get(columnName) == null) {
				if(accumulatorType == 1){
					if(accumulatorLong!=null) {
						minMap.put(columnName, new Double(accumulatorLong));
					}
				} else if (accumulatorType == 4) {
					if(accumulatorDouble!=null) {
						minMap.put(columnName, accumulatorDouble);
					}
				}
 			} else {
				if(accumulatorType == 1){
					minMap.put(columnName, (accumulatorLong> minMap.get(columnName)) ? minMap.get(columnName) : accumulatorLong);
				} else if(accumulatorType == 4){
					minMap.put(columnName, (accumulatorDouble> minMap.get(columnName)) ? minMap.get(columnName) : accumulatorDouble); 
				}
 			}
 		}
		TableDataModelling.expressionAlias = null;

	}

	@Override
	public void visit(InverseExpression inverseExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JdbcParameter jdbcParameter) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DoubleValue doubleValue) {
		accumulatorDouble = doubleValue.getValue();
		accumulatorType = 4;
	}

	@Override
	public void visit(LongValue longValue) {
		accumulatorLong = longValue.getValue();
		accumulatorType = 1;
	}

	@Override
	public void visit(DateValue dateValue) {
		System.out.println(dateValue);
	}

	@Override
	public void visit(TimeValue timeValue) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimestampValue timestampValue) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Parenthesis parenthesis) {
		parenthesis.getExpression().accept(this);
	}

	@Override
	public void visit(StringValue stringValue) {
		accumulatorString = stringValue.getValue();
		accumulatorType = 2;
		
	}

	@Override
	public void visit(Addition addition) {
		addition.getLeftExpression().accept(this);
		if(accumulatorType == 1) {
			long leftExpression = accumulatorLong;
			addition.getRightExpression().accept(this);
			if(accumulatorType == 4) {
				double rightExpression = accumulatorDouble;
				accumulatorDouble = leftExpression + rightExpression;
				accumulatorType = 4;
			} else {
				long rightExpression = accumulatorLong;
				accumulatorLong = leftExpression + rightExpression;
				accumulatorType = 1;
			}
		} else if(accumulatorType == 4) {
			double leftExpression = accumulatorDouble;
			addition.getRightExpression().accept(this);
			if(accumulatorType == 4) {
				double rightExpression = accumulatorDouble;
				accumulatorDouble = leftExpression + rightExpression;
				accumulatorType = 4;
			} else {
				long rightExpression = accumulatorLong;
				accumulatorDouble = leftExpression + rightExpression;
				accumulatorType = 4;
			}
		}
	}

	@Override
	public void visit(Division division) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Multiplication multiplication) {
		multiplication.getLeftExpression().accept(this);
		if(accumulatorType == 1) {
			long leftExpression = accumulatorLong;
			multiplication.getRightExpression().accept(this);
			if(accumulatorType == 4) {
				double rightExpression = accumulatorDouble;
				accumulatorDouble = leftExpression * rightExpression;
				accumulatorType = 4;
			} else {
				long rightExpression = accumulatorLong;
				accumulatorLong = leftExpression * rightExpression;
				accumulatorType = 1;
			}
		} else if(accumulatorType == 4) {
			double leftExpression = accumulatorDouble;
			multiplication.getRightExpression().accept(this);
			if(accumulatorType == 4) {
				double rightExpression = accumulatorDouble;
				accumulatorDouble = leftExpression * rightExpression;
				accumulatorType = 4;
			} else {
				long rightExpression = accumulatorLong;
				accumulatorDouble = leftExpression * rightExpression;
				accumulatorType = 4;
			}
		}
	}

	@Override
	public void visit(Subtraction subtraction) {
		subtraction.getLeftExpression().accept(this);
		if(accumulatorType == 1) {
			long leftExpression = accumulatorLong;
			subtraction.getRightExpression().accept(this);
			if(accumulatorType == 4) {
				double rightExpression = accumulatorDouble;
				accumulatorDouble = leftExpression - rightExpression;
				accumulatorType = 4;
			} else {
				long rightExpression = accumulatorLong;
				accumulatorLong = leftExpression - rightExpression;
				accumulatorType = 1;
			}
		} else if(accumulatorType == 4) {
			double leftExpression = accumulatorDouble;
			subtraction.getRightExpression().accept(this);
			if(accumulatorType == 4) {
				double rightExpression = accumulatorDouble;
				accumulatorDouble = leftExpression - rightExpression;
				accumulatorType = 4;
			} else {
				long rightExpression = accumulatorLong;
				accumulatorDouble = leftExpression - rightExpression;
				accumulatorType = 4;
			}
		}
	}

	@Override
	public void visit(AndExpression andExpression) {
		andExpression.getLeftExpression().accept(this);
		boolean leftExpressionValue = accumulatorBoolean;
		andExpression.getRightExpression().accept(this);
		boolean rightExpressionValue = accumulatorBoolean;
		accumulatorBoolean = leftExpressionValue && rightExpressionValue;
	}

	@Override
	public void visit(OrExpression orExpression) {
		orExpression.getLeftExpression().accept(this);
		boolean leftExpressionValue = accumulatorBoolean;
		orExpression.getRightExpression().accept(this);
		boolean rightExpressionValue = accumulatorBoolean;
		accumulatorBoolean = leftExpressionValue || rightExpressionValue;
	}

	@Override
	public void visit(Between between) {
		between.getLeftExpression().accept(this);
		between.getBetweenExpressionStart().accept(this);
		between.getBetweenExpressionEnd().accept(this);
	}

	@Override
	public void visit(EqualsTo equalsTo) {
		equalsTo.getLeftExpression().accept(this);
		if(columnIndex == -1) {
			accumulatorBoolean = true;
			columnIndex = 0;
			return;
		}

		if(accumulatorType == 1) {
			long leftExpression = accumulatorLong;
			equalsTo.getRightExpression().accept(this);
			if(accumulatorType == 4) {
				double rightExpression = accumulatorDouble;
				accumulatorBoolean = true;
				if(leftExpression != rightExpression) {
					accumulatorBoolean = false;
				}
			} else {
				long rightExpression = accumulatorLong;
				accumulatorBoolean = true;
				if(leftExpression != rightExpression) {
					accumulatorBoolean = false;
				}
			}
		} else if(accumulatorType == 3) {
			Date leftExpression = accumulatorDate;
			equalsTo.getRightExpression().accept(this);
			Date rightExpression = accumulatorDate;
			accumulatorBoolean = true;
			if(leftExpression.compareTo(rightExpression) != 0) {
				accumulatorBoolean = false;
			}
		} else if(accumulatorType == 4) {
			double leftExpression = accumulatorDouble;
			equalsTo.getRightExpression().accept(this);
			if(accumulatorType == 4) {
				double rightExpression = accumulatorDouble;
				accumulatorBoolean = true;
				if((leftExpression*100)/100 != (rightExpression*100)/100) {
					accumulatorBoolean = false;
				}
			} else {
				long rightExpression = accumulatorLong;
				accumulatorBoolean = true;
				if(leftExpression != rightExpression) {
					accumulatorBoolean = false;
				}
			}
		} else if(accumulatorType == 2) {
			String leftExpression = accumulatorString;
			equalsTo.getRightExpression().accept(this);
			String rightExpression = accumulatorString;
			if(!(leftExpression.equals(rightExpression))) {
				accumulatorBoolean = false;
			}
		}
	}

	@Override
	public void visit(GreaterThan greaterThan) {
		greaterThan.getLeftExpression().accept(this);
		if(columnIndex == -1) {
			accumulatorBoolean = true;
			columnIndex = 0;
			return;
		}

		if(accumulatorType == 1) {
			long leftExpression = accumulatorLong;
			greaterThan.getRightExpression().accept(this);
			if(accumulatorType == 4) {
				double rightExpression = accumulatorDouble;
				accumulatorBoolean = true;
				if(leftExpression <= rightExpression) {
					accumulatorBoolean = false;
				}
			} else {
				long rightExpression = accumulatorLong;
				accumulatorBoolean = true;
				if(leftExpression <= rightExpression) {
					accumulatorBoolean = false;
				}
			}
		} else if(accumulatorType == 3) {
			Date leftExpression = accumulatorDate;
			greaterThan.getRightExpression().accept(this);
			Date rightExpression = accumulatorDate;
			accumulatorBoolean = true;
			if(leftExpression.compareTo(rightExpression) <= 0) {
				accumulatorBoolean = false;
			}
		} else if(accumulatorType == 4) {
			double leftExpression = accumulatorDouble;
			greaterThan.getRightExpression().accept(this);
			if(accumulatorType == 4) {
				double rightExpression = accumulatorDouble;
				accumulatorBoolean = true;
				if((leftExpression*100)/100 <= (rightExpression*100)/100) {
					accumulatorBoolean = false;
				}
			} else {
				long rightExpression = accumulatorLong;
				accumulatorBoolean = true;
				if(leftExpression <= rightExpression) {
					accumulatorBoolean = false;
				}
			}
		}
	}

	@Override
	public void visit(GreaterThanEquals greaterThanEquals) {
		greaterThanEquals.getLeftExpression().accept(this);
		if(columnIndex == -1) {
			accumulatorBoolean = true;
			columnIndex = 0;
			return;
		}

		if(accumulatorType == 1) {
			long leftExpression = accumulatorLong;
			greaterThanEquals.getRightExpression().accept(this);
			if(accumulatorType == 4) {
				double rightExpression = accumulatorDouble;
				accumulatorBoolean = true;
				if(leftExpression < rightExpression) {
					accumulatorBoolean = false;
				}
			} else {
				long rightExpression = accumulatorLong;
				accumulatorBoolean = true;
				if(leftExpression < rightExpression) {
					accumulatorBoolean = false;
				}
			}
		} else if(accumulatorType == 3) {
			Date leftExpression = accumulatorDate;
			greaterThanEquals.getRightExpression().accept(this);
			Date rightExpression = accumulatorDate;
			accumulatorBoolean = true;
			if(leftExpression.compareTo(rightExpression) < 0) {
				accumulatorBoolean = false;
			}
		} else if(accumulatorType == 4) {
			double leftExpression = accumulatorDouble;
			greaterThanEquals.getRightExpression().accept(this);
			if(accumulatorType == 4) {
				double rightExpression = accumulatorDouble;
				accumulatorBoolean = true;
				if((leftExpression*100)/100 < (rightExpression*100)/100) {
					accumulatorBoolean = false;
				}
			} else {
				long rightExpression = accumulatorLong;
				accumulatorBoolean = true;
				if(leftExpression < rightExpression) {
					accumulatorBoolean = false;
				}
			}
		}
	}
	
/*	void greaterThan(String columnName, long value) {
		TableSchema tableSchema = getTableSchema();
		List<String[]> tableData = getTableData();
		List<String> columnNames = Helper.getColumnNames(tableSchema);
		ListIterator<String[]> tupleIterator = tableData.listIterator();
		
		int columnIndex = columnNames.indexOf(columnName);
        while(tupleIterator.hasNext()){
        	String[] tuple = tupleIterator.next();
			if(Integer.parseInt(tuple[columnIndex]) < value) {
				tupleIterator.remove();
			}
		}
		updateTableData(tableData);
	}
*/		
	@Override
	public void visit(InExpression inExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(IsNullExpression isNullExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LikeExpression likeExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(MinorThan minorThan) {
		minorThan.getLeftExpression().accept(this);
		if(columnIndex == -1) {
			accumulatorBoolean = true;
			columnIndex = 0;
			return;
		}
		if(accumulatorType == 1) {
			long leftExpression = accumulatorLong;
			minorThan.getRightExpression().accept(this);
			if(accumulatorType == 4) {
				double rightExpression = accumulatorDouble;
				accumulatorBoolean = true;
				if(leftExpression >= rightExpression) {
					accumulatorBoolean = false;
				}
			} else {
				long rightExpression = accumulatorLong;
				accumulatorBoolean = true;
				if(leftExpression >= rightExpression) {
					accumulatorBoolean = false;
				}
			}
		} else if(accumulatorType == 3) {
			Date leftExpression = accumulatorDate;
			minorThan.getRightExpression().accept(this);
			Date rightExpression = accumulatorDate;
			accumulatorBoolean = true;
			if(leftExpression.compareTo(rightExpression) >= 0) {
				accumulatorBoolean = false;
			}
		} else if(accumulatorType == 4) {
			double leftExpression = accumulatorDouble;
			minorThan.getRightExpression().accept(this);
			if(accumulatorType == 4) {
				double rightExpression = accumulatorDouble;
				accumulatorBoolean = true;
				if((leftExpression*100)/100 >= (rightExpression*100)/100) {
					accumulatorBoolean = false;
				}
			} else {
				long rightExpression = accumulatorLong;
				accumulatorBoolean = true;
				if(leftExpression >= rightExpression) {
					accumulatorBoolean = false;
				}
			}
		}
	}

	@Override
	public void visit(MinorThanEquals minorThanEquals) {
		minorThanEquals.getLeftExpression().accept(this);
		if(columnIndex == -1) {
			accumulatorBoolean = true;
			columnIndex = 0;
			return;
		}

		if(accumulatorType == 1) {
			long leftExpression = accumulatorLong;
			minorThanEquals.getRightExpression().accept(this);
			if(accumulatorType == 4) {
				double rightExpression = accumulatorDouble;
				accumulatorBoolean = true;
				if(leftExpression > rightExpression) {
					accumulatorBoolean = false;
				}
			} else {
				long rightExpression = accumulatorLong;
				accumulatorBoolean = true;
				if(leftExpression > rightExpression) {
					accumulatorBoolean = false;
				}
			}
		} else if(accumulatorType == 3) {
			Date leftExpression = accumulatorDate;
			minorThanEquals.getRightExpression().accept(this);
			Date rightExpression = accumulatorDate;
			accumulatorBoolean = true;
			if(leftExpression.compareTo(rightExpression) > 0) {
				accumulatorBoolean = false;
			}
		} else if(accumulatorType == 4) {
			double leftExpression = accumulatorDouble;
			minorThanEquals.getRightExpression().accept(this);
			if(accumulatorType == 4) {
				double rightExpression = accumulatorDouble;
				accumulatorBoolean = true;
				if((leftExpression*100)/100 > (rightExpression*100)/100) {
					accumulatorBoolean = false;
				}
			} else {
				long rightExpression = accumulatorLong;
				accumulatorBoolean = true;
				if(leftExpression > rightExpression) {
					accumulatorBoolean = false;
				}
			}
		}
	}

	@Override
	public void visit(NotEqualsTo notEqualsTo) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Column tableColumn) {
		String columnName = tableColumn.getColumnName();
		String tableName = tableColumn.getTable().getName().toUpperCase();
		if(tableColumn.getTable().getAlias() != null) {
			tableName = tableColumn.getTable().getAlias();
		}
		
		if(tableName == null) { // For the Simple Query column does not have Table Name
			tableName = getTableName().toUpperCase();
		}
		
		if(updatedTableName!=null) {
			tableName = updatedTableName;
			//columnName = tableColumn.getWholeColumnName();
		}
		
		TableSchema tableSchema =  getTableSchema(tableName);

		List<String> columnNames = Helper.getColumnNames(tableSchema);
		
		/* We need to handle the case where an alias for a Table is created while the colukn is accessed by Table name
		 * SELECT PLAYERS.ID, P2.id FROM PLAYERS P1 JOIN personInfo P2 on P1.ID >= P2.id
		 */
		columnIndex = columnNames.indexOf(columnName);
		if(tableName.contains("_") && (!tableName.equals("SUBSELECT_TABLE"))) {
			columnIndex = columnNames.indexOf(tableColumn.getTable().getName().toUpperCase()+"."+columnName);
		}
		accumulatorBoolean = true;
		if(columnIndex != -1) {
		
		String dataType = tableSchema.getColumnDefinitions().get(columnIndex).getColDataType().getDataType();
		if(dataType.equalsIgnoreCase("int")) {
			accumulatorLong = Long.parseLong(tuple[columnIndex]);
			accumulatorType = 1;
		} else if (dataType.equalsIgnoreCase("string") || dataType.equalsIgnoreCase("char") || dataType.equalsIgnoreCase("varchar")) {
			accumulatorString = tuple[columnIndex];
			accumulatorType = 2;
 		} else if (dataType.equalsIgnoreCase("date")) {
 			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
 			try {
				accumulatorDate = dateFormat.parse(tuple[columnIndex]);
				accumulatorType = 3;
			} catch (ParseException e) {
				System.out.println("Illegal Date format. Require yyyy-MM-dd. "+e.getMessage());
			}
		} else if (dataType.equalsIgnoreCase("decimal")) {
			accumulatorDouble = Double.parseDouble(tuple[columnIndex]);
			accumulatorType = 4;
 		}
		}
	}

	@Override
	public void visit(SubSelect subSelect) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CaseExpression caseExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WhenClause whenClause) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ExistsExpression existsExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllComparisonExpression allComparisonExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AnyComparisonExpression anyComparisonExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Concat concat) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Matches matches) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseAnd bitwiseAnd) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseOr bitwiseOr) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseXor bitwiseXor) {
		// TODO Auto-generated method stub
		
	}
	
	// Require Update so that we need not change table name to upper case everywhere
	public TableSchema getTableSchema(String tableName) {
		Map<String, TableSchema> allTableSchema =  tableMetaDataService.getAllTableSchema();
		TableSchema currentTableSchema = allTableSchema.get(tableName);
		return currentTableSchema;
	}
	
/*	public void setTableSchema(TableSchema tableSchema) {
		this.tableSchema = tableSchema;
	}
*/	
	public void updateTableData(List<String[]> tableData) {
		tableDataService.setTableData(tableData);
	}

}
