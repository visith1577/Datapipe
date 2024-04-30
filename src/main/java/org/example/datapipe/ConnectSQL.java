package org.example.datapipe;

import java.sql.*;

import io.github.cdimascio.dotenv.Dotenv;

public class ConnectSQL{
    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/utilitySaga?useSSL=false&allowPublicKeyRetrieval=true";
    public static final Dotenv dotenv = Dotenv
            .configure()
            .load();

    public static Connection getConnection() throws SQLException {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String user = dotenv.get("DB_USERNAME");
            String password = dotenv.get("DB_PASSWORD");
            return DriverManager.getConnection(JDBC_URL, user, password);
        } catch (ClassNotFoundException e) {
            throw new SQLException("Database driver not found", e);
        }
    }

    public static void closeConnection(Connection connection) throws SQLException {
        if (connection != null ) {
            connection.close();
        }
    }
}

