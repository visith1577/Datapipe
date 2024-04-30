package Task;

import org.example.datapipe.ConnectSQL;



import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TimedTask {
    private Double getMonthlyBill(String account, String cat) throws SQLException {

        Helpers dao = new Helpers();

        String id = dao.getIotIdForAccount(account, cat);

        List<IoTModel> data_list_daily = dao.getFinalReadingsDailyForCurrentMonth(id);

        int minData = Integer.MAX_VALUE;
        int maxData = Integer.MIN_VALUE;

        for (IoTModel model : data_list_daily) {
            int data = model.getData();
            if (data < minData) {
                minData = data;
            }
            if (data > maxData) {
                maxData = data;
            }
        }

        int difference = maxData - minData;

        double bill;
        switch (cat.toUpperCase()) {
            case "WATER":
                bill = dao.calculateDomesticBillWater(difference);
                break;
            case "ELECTRICITY":
                bill = dao.calculateDomesticBill(difference, 0, 0);
                break;
            default:
                throw new IllegalArgumentException("Invalid category name: " + cat);
        }

        return bill;
    }

    private List<String> getAccountsWithIotMeter(String category) throws SQLException {
        String tableName;
        switch (category.toUpperCase()) {
            case "WATER":
                tableName = "wAccount_list";
                break;
            case "ELECTRICITY":
                tableName = "eAccount_list";
                break;
            default:
                throw new IllegalArgumentException("Invalid category name: " + category);
        }
        List<String> accounts = new ArrayList<>();
        String query = "SELECT account_number FROM "+ tableName +" WHERE iot_meter = ?";
        Connection connection = ConnectSQL.getConnection();

        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {

            preparedStatement.setString(1, "YES");
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                accounts.add(resultSet.getString("account_number"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return accounts;
    }

    private void updateBalance(String accountNumber, Double bill, String category, Connection connection) throws SQLException {
        String tableName;
        switch (category.toUpperCase()) {
            case "WATER":
                tableName = "wAccount_list";
                break;
            case "ELECTRICITY":
                tableName = "eAccount_list";
                break;
            default:
                throw new IllegalArgumentException("Invalid category name: " + category);
        }
        String queryGet = "SELECT balance FROM "+ tableName +" WHERE account_number = ?";
        String queryUpdate = "UPDATE "+ tableName +" SET balance = ? WHERE account_number = ?";

        try (PreparedStatement preparedStatementGet = connection.prepareStatement(queryGet)) {
            preparedStatementGet.setString(1, accountNumber);
            ResultSet resultSet = preparedStatementGet.executeQuery();
            if (resultSet.next()) {
                Double currentBalance = resultSet.getDouble("balance");
                double newBalance = currentBalance + bill;

                try (PreparedStatement preparedStatementUpdate = connection.prepareStatement(queryUpdate)) {
                    preparedStatementUpdate.setDouble(1, newBalance);
                    preparedStatementUpdate.setString(2, accountNumber);
                    preparedStatementUpdate.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void addBill(String accountNumber, Double amount, String category, Connection connection) throws SQLException {
        String billId = UUID.randomUUID().toString();
        String tableName;

        switch (category.toUpperCase()) {
            case "WATER":
                tableName = "water_bill";
                break;
            case "ELECTRICITY":
                tableName = "electricity_bill";
                break;
            default:
                throw new IllegalArgumentException("Invalid category name: " + category);
        }

        String query = "INSERT INTO " + tableName + " (billId, account_number, amount, billedDate, status) VALUES (?, ?, ?, ?, ?)";

        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setString(1, billId);
            preparedStatement.setString(2, accountNumber);
            preparedStatement.setDouble(3, amount);
            preparedStatement.setDate(4, new java.sql.Date(System.currentTimeMillis()));
            preparedStatement.setString(5, "PENDING");
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public void updateAccountBalances(String category){
        Connection connection = null;

        try {
            connection = ConnectSQL.getConnection();
            connection.setAutoCommit(false);

            List<String> accounts = getAccountsWithIotMeter(category);
            System.out.println("Accounts with IoT meter: " + accounts);

            for (String account : accounts) {
                System.out.println("Updating balance for account: " + account);
                Double bill = getMonthlyBill(account, category);
                updateBalance(account, bill, category, connection);
                addBill(account, bill, category, connection);
            }
            connection.commit();
        } catch (SQLException e) {
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.setAutoCommit(true);
                    ConnectSQL.closeConnection(connection);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
