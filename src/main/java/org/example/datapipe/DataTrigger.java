package org.example.datapipe;

import java.io.*;
import java.util.concurrent.atomic.AtomicReference;

import callbacks.MeterCallback;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.*;
import jakarta.servlet.annotation.*;
import org.eclipse.paho.client.mqttv3.MqttException;

@WebServlet(loadOnStartup = 1, urlPatterns = "/activate")
public class DataTrigger extends HttpServlet {

    private MQTTClient mqttClient;
    private final AtomicReference<String> latestReading = new AtomicReference<>();
    private final AtomicReference<String> deviceID = new AtomicReference<>();


    @Override
    public void init() throws ServletException {
        try {
            mqttClient = new MQTTClient();
            mqttClient.setCallback(new MeterCallback(latestReading, deviceID));
        } catch (MqttException e) {
            throw new ServletException("Failed to initialize MQTT client", e);
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String reading = latestReading.get();
        if (reading != null) {
            resp.getWriter().write("Latest reading: " + reading);
        } else {
            resp.getWriter().write("No data received yet");
        }
    }

    @Override
    public void destroy() {
        try {
            mqttClient.disconnect();
        } catch (MqttException e) {
            System.out.println(e.getMessage());
        }
    }
}