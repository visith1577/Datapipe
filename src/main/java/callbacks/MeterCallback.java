package callbacks;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class MeterCallback implements MqttCallback {

    private final AtomicReference<String> latestReading;
    private final AtomicReference<String> deviceId;

    public MeterCallback(AtomicReference<String> latestReading, AtomicReference<String> deviceId) {
        this.latestReading = latestReading;
        this.deviceId = deviceId;
    }

    @Override
    public void connectionLost(Throwable throwable) {
        System.out.println("Connection lost: " + throwable.getMessage());
    }

    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) {
        String payload = new String(mqttMessage.getPayload());

        String[] parts = topic.split("/");
        String Id = parts[parts.length - 1];

        System.out.println("Received electricity meter reading: " + payload + " from device: " + Id);
        processReading(payload, Id);
        if (mqttMessage.isRetained()) {
            System.out.println("messages retained");
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        // Handle delivery complete
        System.out.println("Delivery complete for message: " + iMqttDeliveryToken.getMessageId());
    }

    private void processReading(String payload, String Id) {
        try {
            getScheduledReading(payload, Id);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Processing reading: " + payload);
        // ...
    }


    private void getScheduledReading(String load, String Id) throws SQLException {
        latestReading.set(load);
        deviceId.set(Id);
        Runnable readingSaverTask = new ReadingSaver(latestReading, deviceId);
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(readingSaverTask, 0, 10, TimeUnit.SECONDS);
    }
}
