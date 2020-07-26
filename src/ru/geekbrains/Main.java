package ru.geekbrains;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Scanner;

//        1. Создать CRUD операции, 1 метод создани таблицы 2 метод для добавления записи
//        3 метод для получения записи 4 метод для удаления записи 5 удаление таблицы

//        2. Обновить данные в БД из файла (файл приложен test.txt)

public class Main
{

    private static Connection connection;
    private static Statement statement;
    private static PreparedStatement pstmt;


    public static void main(String[] args)
    {
	// write your code here
        try
        {
            connect();
        }
        catch (ClassNotFoundException e)
        {
            e.printStackTrace();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }

        try
        {
            // Важно!!! Если добавлять из файла, то обязательно на чистой таблице этот вызов.
            // Ибо в файле даны определённые ID. Как я понял, только при соблюдении всех трёх условий запись обновляется?!
            // Во всяком случае я из этого исходил.
//            statement.executeUpdate("delete from sqlite_sequence where name = 'students2'");   //!!!

            // создание таблицы.
            createTab("CREATE TABLE students2 (" +
                    " id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                            " name TEXT, " +
                            " score INTEGER )");

            // Добавление записей.
            System.out.println(addToTab("insert into students2 (name, score) values ('Bob1', 10)"));
            System.out.println(addToTab("insert into students2 (name, score) values ('Bob2', 20)"));
            System.out.println(addToTab("insert into students2 (name, score) values ('Bob3', 30)"));

            //  Получение записей.
            selectFromTab("select * from students2");

            // Удаление записей.
            System.out.println(deleteFromTable("delete from students2"));

            // Удаление таблицы.
            System.out.println(dropTheTable("students2"));


            // Обновление данных в БД из файла (файл приложен test.txt)
            readAndUpdate("test.txt");


        } catch (SQLException e) {
            e.printStackTrace();
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        disconnect();
    }


    public static void connect() throws ClassNotFoundException, SQLException {
        Class.forName("org.sqlite.JDBC");
        connection = DriverManager.getConnection("jdbc:sqlite:main");
        statement = connection.createStatement();
    }

    public static void disconnect()
    {
        try {
            connection.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static int createTab(String str) throws SQLException
    {
        return statement.executeUpdate(str);
    }

    public static int addToTab(String str) throws SQLException
    {
        return statement.executeUpdate(str);

    }

    public static void selectFromTab(String str) throws SQLException
    {
        ResultSet rs = statement.executeQuery(str);
        ResultSetMetaData rsmd = rs.getMetaData();
        System.out.println("Таблица: " + rsmd.getTableName(1));
        while (rs.next())
        {
            for (int i = 1; i <= rsmd.getColumnCount(); i++)
            {
                System.out.print(rsmd.getColumnName(i) + ":");
                if(rsmd.getColumnType(i) == 4)                     // Достаточно топорно, понимаю.
                {
                    System.out.print(rs.getInt(i) + " ");
                }
                else if(rsmd.getColumnType(i) == 12)
                {
                    System.out.print(rs.getString(i) + " ");
                }
            }
            System.out.print("\n");
        }


    }

    public static int deleteFromTable(String str) throws SQLException
    {
        return statement.executeUpdate(str);
    }

    public static int dropTheTable(String name) throws SQLException
    {
        return statement.executeUpdate("DROP TABLE IF EXISTS " + name);
    }

    public static void readAndUpdate(String fileName) throws IOException, SQLException {
        FileReader fileReader = new FileReader(fileName);
        Scanner scanner = new Scanner(fileReader);
        pstmt = connection.prepareStatement("update students2 set name = ?, score = ? where id = ?");
        Savepoint svp = connection.setSavepoint();
        while (scanner.hasNextLine())
        {
            String[] token = scanner.nextLine().split(" ");
            pstmt.setString(1, token[1]);
            pstmt.setInt(2, Integer.parseInt(token[2]));
            pstmt.setInt(3, Integer.parseInt(token[0]));
            if(pstmt.executeUpdate() == 0)
            {
                connection.rollback(svp);
                System.out.println("Транзакция записи отменена.");
                break;
            }
        }
        connection.setAutoCommit(true);

        fileReader.close();
    }


}
