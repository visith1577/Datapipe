package callbacks;

import org.example.datapipe.ConnectSQL;

import java.sql.*;
import java.util.concurrent.atomic.AtomicReference;

public class ReadingSaver implements Runnable{

    private final AtomicReference<String> latestReading;
    private final AtomicReference<String> device;

    public ReadingSaver(AtomicReference<String> latestReading, AtomicReference<String> device) {
        this.latestReading = latestReading;
        this.device = device;
    }

    @Override
    public void run() {
        String reading = latestReading.getAndSet(null);
        String deviceId = device.getAndSet(null);
        if (reading != null) {
            try {
                saveReadingToDatabase(reading, deviceId);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            latestReading.set(null);
            device.set(null);
        }
    }

    private void saveReadingToDatabase(String reading, String device) throws SQLException {
        Connection connection = ConnectSQL.getConnection();
        String table = device + "_meter";
        try {
            DatabaseMetaData dbm = connection.getMetaData();
            ResultSet tables = dbm.getTables(null, null, table, null);
            if (!tables.next()) {
                return;
            }
            PreparedStatement stmt = connection.prepareStatement("INSERT INTO " + table +"(data) VALUES (?)");

            stmt.setString(1, reading);
            stmt.executeUpdate();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectSQL.closeConnection(connection);
        }
    }
}
