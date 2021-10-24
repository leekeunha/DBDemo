package mysqlConnector;

import java.util.Scanner;
import java.sql.*;

public class Connector {

	private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
	private static final String DB_URL= "jdbc:mysql://localhost:3306/jdbc";
//	private static String USER_NAME= "";
//	private static String PASSWORD= "";
	private static String USER_NAME= "connector";
	private static String PASSWORD= "jdbc@2021";
	

	public static void main(String[] args) {
		try {
			Class.forName(JDBC_DRIVER);
			System.out.println("connect the database...");
			/*
			 * Scanner scan = new Scanner(System.in); System.out.println("USER_NAME");
			 * USER_NAME = scan.nextLine(); System.out.println("PASSWORD"); PASSWORD =
			 * scan.nextLine();
			 */

			Connection connection = DriverManager.getConnection(DB_URL,USER_NAME,PASSWORD);
			QueryExecutor queryExecutor = new QueryExecutor(connection);
			System.out.println("connection successful!");
			control(connection,queryExecutor);
			System.out.println("end the connection");
			connection.close();

		} catch (Exception e) {
			System.out.println("connection fail!");
			System.out.println("Exception" + e);			
		}
	}

	public static void control(Connection connection,QueryExecutor queryExecutor) {
		try {
			
			while (true) {
				Scanner scan = new Scanner(System.in);
				System.out.println("\nPlease input the instruction number(1:Show Tables, 2:CreateTable,3:Select,4:insert,5:Delete,6:Update,7:Drop Table, 8:Exit):");
				int operate_id = scan.nextInt();
				
				switch (operate_id) {
				
				case 1: 
					queryExecutor.showTables();
					break;
				case 2:
					queryExecutor.createTable();
					break;
				case 3: 
					queryExecutor.select();
					break;
				case 4:
					queryExecutor.insert();
					break;
				case 5:
					queryExecutor.delete();
					break;
				case 6:
					queryExecutor.update();
					break;
				case 7:
					queryExecutor.dropTable();
					break;
				case 8:				
					return;
				default: 
					System.out.println("\nInvalid number");
	            break;
				}
			}
		} catch (Exception e) {
			System.out.println("Exception" + e);	
		}
	}
}
