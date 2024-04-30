package org.example.datapipe;

import io.github.cdimascio.dotenv.Dotenv;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import javax.net.ssl.SSLSocketFactory;
import java.util.Objects;

public class MQTTClient {
    public static final Dotenv dotenv = Dotenv
            .configure()
            .load();
    private static final String BROKER_URL = "ssl://" +  dotenv.get("HIVE_URL") + ":" + dotenv.get("HIVE_PORT");
    private static final String TOPIC_ELEC = dotenv.get("TOPIC_ELEC");
    private static final String TOPIC_WATER = dotenv.get("TOPIC_WATER");
    private final MqttClient mqttClient;


    public MQTTClient() throws MqttException {
        mqttClient = new MqttClient(BROKER_URL, MqttClient.generateClientId(), new MemoryPersistence());
        MqttConnectOptions options = new MqttConnectOptions();
        options.setUserName(dotenv.get("HIVE_USER"));
        options.setPassword(Objects.requireNonNull(dotenv.get("HIVE_PASS")).toCharArray());

        options.setSocketFactory(SSLSocketFactory.getDefault());
        options.setCleanSession(false);
        mqttClient.connect(options);
        mqttClient.subscribe(TOPIC_ELEC, 2, null);
        mqttClient.subscribe(TOPIC_WATER, 2, null);
    }

    public void setCallback(MqttCallback callback) throws MqttException {
        mqttClient.setCallback(callback);
    }

    public void disconnect() throws MqttException {
        mqttClient.disconnect();
    }
}
