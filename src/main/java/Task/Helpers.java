package Task;

import org.example.datapipe.ConnectSQL;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Helpers {

    public double calculateDomesticBill(double unit1, double unit2, double unit3) {
        double bill = 0.0;
        double fixedCharge = 0.0;
        boolean isTimeOfUse = (unit2 > 0 || unit3 > 0);

        if (isTimeOfUse) {

            bill += unit1 * 70.0; // Day Time
            bill += unit2 * 90.0; // Peak Time
            bill += unit3 * 30.0; // Off-Peak Time
            fixedCharge = 2000.0;
        } else {

            double totalUnits = unit1;
            double remainingUnits = totalUnits;

            if (totalUnits <= 30) {
                bill = totalUnits * 8.0;
                fixedCharge = 150.0;
            } else if (totalUnits <= 60) {
                bill = 30 * 8.0 + (totalUnits - 30) * 20.0;
                fixedCharge = 300.0;
            } else {
                remainingUnits = totalUnits - 60;
                bill = 30 * 8.0 + 30 * 20.0; // For the first 60 units

                if (remainingUnits <= 30) {
                    bill += remainingUnits * 25.0;
                } else if (remainingUnits <= 60) {
                    bill += 30 * 25.0 + (remainingUnits - 30) * 30.0;
                    fixedCharge = 400.0;
                } else if (remainingUnits <= 120) {
                    bill += 30 * 25.0 + 30 * 30.0 + (remainingUnits - 60) * 50.0;
                    fixedCharge = 1000.0;
                } else if (remainingUnits <= 180) {
                    bill += 30 * 25.0 + 30 * 30.0 + 60 * 50.0 + (remainingUnits - 120) * 50.0;
                    fixedCharge = 1500.0;
                } else {
                    bill += 30 * 25.0 + 30 * 30.0 + 60 * 50.0 + 60 * 50.0 + (remainingUnits - 180) * 75.0;
                    fixedCharge = 2000.0;
                }
            }
        }

        double amt =  bill + fixedCharge;
        double tax = amt * 0.025;

        amt += tax;
        return amt;
    }

    public double calculateDomesticBillWater(double unit) {
        double bill = 0.0;
        double fixedCharge = 0.0;

        if (unit <= 5) {
            bill = unit * 60.0;
            fixedCharge = 300.0;
        } else if (unit <= 10) {
            bill = 5 * 60.0 + (unit - 5) * 80.0;
            fixedCharge = 300.0;
        } else if (unit <= 15) {
            bill = 5 * 60.0 + 5 * 80.0 + (unit - 10) * 100.0;
            fixedCharge = 300.0;
        } else if (unit <= 20) {
            bill = 5 * 60.0 + 5 * 80.0 + 5 * 100.0 + (unit - 15) * 110.0;
            fixedCharge = 400.0;
        } else if (unit <= 25) {
            bill = 5 * 60.0 + 5 * 80.0 + 5 * 100.0 + 5 * 110.0 + (unit - 20) * 130.0;
            fixedCharge = 500.0;
        } else if (unit <= 30) {
            bill = 5 * 60.0 + 5 * 80.0 + 5 * 100.0 + 5 * 110.0 + 5 * 130.0 + (unit - 25) * 160.0;
            fixedCharge = 600.0;
        } else if (unit <= 40){
            bill = 5 * 60.0 + 5 * 80.0 + 5 * 100.0 + 5 * 110.0 + 5 * 130.0 + 5 * 160.0 + (unit - 30) * 180.0;
            fixedCharge = 1500.0;
        } else if (unit <= 50){
            bill = 5 * 60.0 + 5 * 80.0 + 5 * 100.0 + 5 * 110.0 + 5 * 130.0 + 5 * 160.0 + 10 * 180.0 + (unit - 40) * 210.0;
            fixedCharge = 3000.0;
        } else if (unit <= 75) {
            bill = 5 * 60.0 + 5 * 80.0 + 5 * 100.0 + 5 * 110.0 + 5 * 130.0 + 5 * 160.0 + 10 * 180.0 + 10 * 210.0 + (unit - 50) * 240.0;
            fixedCharge = 3500.0;
        } else if (unit <= 100){
            bill = 5 * 60.0 + 5 * 80.0 + 5 * 100.0 + 5 * 110.0 + 5 * 130.0 + 5 * 160.0 + 10 * 180.0 + 10 * 210.0 + 25 * 230.0 + (unit - 75) * 270.0;
            fixedCharge = 4000.0;
        } else {
            bill = 5 * 60.0 + 5 * 80.0 + 5 * 100.0 + 5 * 110.0 + 5 * 130.0 + 5 * 160.0 + 10 * 180.0 + 10 * 210.0 + 25 * 230.0 + 25 * 270.0 + (unit - 100) * 300.0;
            fixedCharge = 4500.0;
        }

        double tax = (bill + fixedCharge) * 0.18;

        return bill + fixedCharge + tax;
    }

    public List<IoTModel> getFinalReadingsDailyForCurrentMonth(String account) throws SQLException {
        List<IoTModel> meterData = new ArrayList<>();
        Connection connection = ConnectSQL.getConnection();

        try {
            String tableName = account + "_meter";
            PreparedStatement statement = connection.prepareStatement(
                    "SELECT date, MIN(time) as time, MIN(data) as data FROM " + tableName +
                            " WHERE MONTH(date) = MONTH(CURDATE()) AND YEAR(date) = YEAR(CURDATE())" +
                            " GROUP BY date"
            );

            dataDriver(meterData, statement);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            ConnectSQL.closeConnection(connection);
        }

        return meterData;
    }

    public String getIotIdForAccount(String account, String category) throws SQLException {
        Connection connection = ConnectSQL.getConnection();
        String iotId = null;

        try {
            String tableName = selectTable(category);

            PreparedStatement statement = connection.prepareStatement(
                    "SELECT iot_id FROM " + tableName + " WHERE account_number = ?"
            );

            statement.setString(1, account);

            try (ResultSet result = statement.executeQuery()) {
                if (result.next()){
                    iotId = result.getString("iot_id");
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            ConnectSQL.closeConnection(connection);
        }
        return iotId;
    }

    private String selectTable(String category) {
        String tableName;
        switch (category.toUpperCase()) {
            case "WATER":
                tableName = "wAccount_list";
                break;
            case "ELECTRICITY":
                tableName = "eAccount_list";
                break;
            default:
                throw new IllegalArgumentException("Invalid table name: " + category);
        }

        return  tableName;
    }

    private void dataDriver(List<IoTModel> meterData, PreparedStatement statement) throws SQLException {
        try (ResultSet result = statement.executeQuery()){
            while (result.next()) {
                IoTModel model = new IoTModel();
                model.setDate(result.getDate("date"));
                model.setTime(result.getTime("time"));
                model.setData(result.getInt("data"));
                meterData.add(model);
            }
        }
    }
}
