package mysqlConnector;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

public class QueryExecutor {
	Connection connection; 
	QueryExecutor(Connection connection){
		this.connection = connection;
	}
	
	public void showTables() throws SQLException {
		try {
			ResultSet resultSet = null;
		    DatabaseMetaData meta = (DatabaseMetaData) this.connection.getMetaData();
		    String catalog = this.connection.getCatalog(); //returns database name
		    resultSet = meta.getTables(catalog, null, null, new String[] {
		         "TABLE"
		      });
		    
		    System.out.println("==============");
		    System.out.println(" Table List");
		    System.out.println("==============");
		    
		    if(resultSet != null) {
		    	while (resultSet.next()) {
		            String tableName = resultSet.getString("TABLE_NAME");
		            System.out.println(tableName);
		         }
		    }
		} catch (Exception e) {
			throw e;
		}		
	}
	
	public void select() throws SQLException {
		try {
			
			Scanner scanner = new Scanner(System.in);
			
			String orderbySql = "";
			
			System.out.println("Insert the table name:");
			String inputTableName = scanner.nextLine();
			
			System.out.println("Insert the column name(ALL: *):");
			String selectColumnName = scanner.nextLine();
			
			Statement statement = this.connection.createStatement();
			
			System.out.println("Insert the columns name for ordering(press enter to skip):");
			String columnNamesForOrdering = scanner.nextLine();
			
			ResultSet resultSet = null;
			String orderBySql = "";
			
			if(columnNamesForOrdering != "")
			{
				String[] sortingCreteriaArray = null; 
				String[] columnNamesArray = columnNamesForOrdering.split(",");
				
				System.out.println("Insert the sorting creteria(press enter to skip):");
				String sortingCreterias = scanner.nextLine();
				
				
				if(sortingCreterias!= "") {
					sortingCreteriaArray = sortingCreterias.split(",");
				}
				
				orderBySql = writeOrderBySql(columnNamesArray,sortingCreteriaArray);
			}
			
			String sql = "SELECT "+ selectColumnName + " FROM " + inputTableName +" " + orderBySql +";";
			
			resultSet = statement.executeQuery(sql);
			ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
			
			int columnCount = resultSetMetaData.getColumnCount();
			String[] columnNames = new String[columnCount]; 
			 
			for(int i=1; i<=columnCount; i++) {
		        // Put column name into array
		        columnNames[i-1] = resultSetMetaData.getColumnName(i); 
		    }
			
			System.out.println("======================");
			
			for (String columnName : columnNames) {
				System.out.print(columnName + "|");
			}
			System.out.println("");
            System.out.println("======================");
	            
			while (resultSet.next()) {
				for (String columnName: columnNames) {
		            System.out.print(resultSet.getObject(columnName) + " "); 
		        }
				System.out.println("");
			}
		} catch (Exception e) {
			throw e;
		}		
	}

	private List<String> getselectColumnNameArray(String selectColumnName) {
		
		List<String> selectColumnNameArray = new ArrayList<String>();
		
		if(selectColumnName.equals("*")) {
			selectColumnNameArray = Arrays.asList(selectColumnName.split("\\s*,\\s*"));
		}
		
		return selectColumnNameArray;
	}

	private String writeOrderBySql(String[] columnNamesArray, String[] sortingCreteriaArray) {
		
		String afterOrderbySql = "";
		List<String> columnNameAndSortingCreteriaArray = new ArrayList<String>();
		
		if (sortingCreteriaArray == null) {
			afterOrderbySql = String.join(",", columnNamesArray);
		}
		else {
			
			for (int i = 0; i < columnNamesArray.length; i++) {
				String columnNameAndSortingCreteria = columnNamesArray[i] +" "+ sortingCreteriaArray[i]; 
				columnNameAndSortingCreteriaArray.add(columnNameAndSortingCreteria);
			}
			
			afterOrderbySql = String.join(",", columnNameAndSortingCreteriaArray);
		}
		
		String orderbySql ="order by " + afterOrderbySql;
		
		return orderbySql;
	}

	public void insert() throws Exception {
		try {
			Scanner scanner = new Scanner(System.in);
			
			System.out.println("Insert the table name:");
			String inputTableName = scanner.nextLine();
			
			System.out.println("Insert the column name:");
			String inputColumnNames = scanner.nextLine();
			String[] columnNamesArray = inputColumnNames.split(",");
			
			System.out.println("Insert the column value:");
			String inputColumnValues = scanner.nextLine();
			String[] columnValuesArray = inputColumnValues.split(",");
			
			String sql = "INSERT INTO " + inputTableName + " (";	
			for (int i = 0; i < columnNamesArray.length;i++) {
				if(i==0) {
					sql += columnNamesArray[i];
				}else {
					sql += ", " + columnNamesArray[i];
				}
			}
			sql +=") VALUES (";
			for (int i = 0; i < columnValuesArray.length;i++) {
				if(i==0) {
					sql += "?";
				}else {
					sql += ",?";
				}
			}
			sql +=");";
			
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			
			for (int i = 0; i < columnValuesArray.length; i++) {
				preparedStatement.setString(i+1, columnValuesArray[i]);
			}
			preparedStatement.executeUpdate();
		} catch (Exception e) {
			throw e;
		}
	}

	public void createTable() throws Exception {
		try {
			Scanner scanner = new Scanner(System.in);
			
			List<String> columnNamesList = new ArrayList<String>();
			List<String> primaryKeyslist = new ArrayList<String>();
			List<String> oneColumnSqlList = new ArrayList<String>();
			
			System.out.println("Insert the table name:");
			String inputTableName = scanner.nextLine();
			
			while (true) {
				
				System.out.println("Insert the column name(press enter to finish):");
				String inputColumnName = scanner.nextLine();

				if(inputColumnName.equals(""))
				{
					break;
				}

				columnNamesList.add(inputColumnName);

				System.out.println("Insert the column type");

				String inputColumnType = scanner.nextLine();

				System.out.println("Insert the column condition(1:primary key, 2:skip):");
				String primarykeyOrSkip = scanner.nextLine();

				if(primarykeyOrSkip == "1") {
					primaryKeyslist.add(inputColumnName);
				}

				System.out.println("Insert the column condition(1:not null, 2:skip):");
				String notNullOrSkip = scanner.nextLine();

				String notNullText = "";

				if(primarykeyOrSkip.equals("1")) {
					primaryKeyslist.add(inputColumnName);
				}
				
				if(notNullOrSkip.equals("1")) {
					notNullText = "not null";
				}
				String oneColumnSql = writeOneColumnSql(inputColumnName, inputColumnType, notNullText);

				oneColumnSqlList.add(oneColumnSql);
			}
			
			String columnNamesSql = String.join(",",  oneColumnSqlList);
			String primaryKeySql = String.join(",",  primaryKeyslist);
			
			String sql = "CREATE TABLE " + inputTableName + " (";
			sql += columnNamesSql + " ";
			
			if(primaryKeySql != "")
			{
				sql += ", primary key (" + primaryKeySql+ ")";		
			}
			
			sql += ");";
			
			PreparedStatement preparedStatement = connection.prepareStatement(sql);

			int result = preparedStatement.executeUpdate();
			
			if(result == 0) {
				System.out.println("table created successfully....");
			} else
			{
				System.out.println("table creation failed");	
			}
                    
			
		} catch (Exception e) {
			throw e;
		}
		
	}

	private String writeOneColumnSql (String inputColumnName, String inputColumnType, String notNullText) {		
		String makeOneColumnSql = inputColumnName +" " + inputColumnType + " "+notNullText;
		return makeOneColumnSql;
	}

	public void dropTable() throws SQLException {
		try {

			Scanner scanner = new Scanner(System.in);
			System.out.println("Insert the table name:");
			String inputTableName = scanner.nextLine();

			System.out.println("Are you sure(Y:yes, N:no)?:");
			String yesOrNo = scanner.nextLine();

			if(yesOrNo.equalsIgnoreCase("y")) {
				PreparedStatement dropTable = this.connection.prepareStatement(
						String.format("DROP TABLE IF EXISTS %s", inputTableName));
				dropTable.execute();
			}

		} catch (SQLException e) {
			throw e;
		}
	}

	public void delete() throws SQLException {
		try {
		Scanner scanner = new Scanner(System.in);
		System.out.println("Insert the table name:");
		String inputTableName = scanner.nextLine();
		
		scanner = new Scanner(System.in);
		System.out.println("Insert the column name:");
		String inputColumnName = scanner.nextLine();
		
		System.out.println("Insert the condition(1:=,2:>,3:<,4:>=,5:<=,6!=):");
		String inputCondition = scanner.nextLine();
		int inputConditionNumber = Integer.parseInt(inputCondition);

		switch (inputConditionNumber) {
		
		case 1:
			inputCondition = "=";
			break;
		case 2:
			inputCondition = ">";
			break;
		case 3:
			inputCondition = "<";
			break;
		case 4:
			inputCondition = ">=";
			break;
		case 5:
			inputCondition = "<=";
			break;
		case 6:
			inputCondition = "!=";
			break;
		default: 
			inputCondition ="";
			break;
		}
		
		System.out.println("Insert the condition value:");
		String inputConditionValue = scanner.nextLine();
		
		String sql = "DELETE FROM "+ inputTableName +" WHERE " + inputColumnName +" "+ inputCondition+" " + inputConditionValue + ";";
		Statement statement = this.connection.createStatement();
		statement.executeUpdate(sql);
		
		System.out.println("delete data successful!");
		
		} catch (SQLException e) {
			throw e;
		}
	}

	public void update() throws SQLException {
		try {
			Scanner scanner = new Scanner(System.in);
			System.out.println("Insert the table name:");
			String inputTableName = scanner.nextLine();
			
			scanner = new Scanner(System.in);
			System.out.println("Insert the column name:");
			String inputColumnName = scanner.nextLine();
			
			System.out.println("Insert the condition(1:=,2:>,3:<,4:>=,5:<=,6!=):");
			String inputCondition = scanner.nextLine();
			int inputConditionNumber = Integer.parseInt(inputCondition);

			switch (inputConditionNumber) {
			
			case 1:
				inputCondition = "=";
				break;
			case 2:
				inputCondition = ">";
				break;
			case 3:
				inputCondition = "<";
				break;
			case 4:
				inputCondition = ">=";
				break;
			case 5:
				inputCondition = "<=";
				break;
			case 6:
				inputCondition = "!=";
				break;
			default: 
				inputCondition ="";
				break;
			}
			
			System.out.println("Insert the condition value:");
			String inputConditionValue = scanner.nextLine();
			
			System.out.println("Insert the desired column:");
			String inputDesiredColumn = scanner.nextLine();
			
			System.out.println("Insert the desired value:");
			String inputDesiredValue = scanner.nextLine();
			
			String sql = "UPDATE "+ inputTableName + " SET " + inputDesiredColumn + " = " + inputDesiredValue + " WHERE " + inputColumnName + " " + inputCondition + " " + "'" + inputConditionValue + "'" + ";";
			
			Statement statement = this.connection.createStatement();
			int updateResult = statement.executeUpdate(sql);
			
			if (updateResult > 0) {
				System.out.println(updateResult+ " rows Updated");
			} else {
				System.out.println("0 rows updated");
			}
			
			} catch (SQLException e) {
				throw e;
			}
	}
}
