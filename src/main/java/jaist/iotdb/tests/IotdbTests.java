/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */
package jaist.iotdb.tests;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import jdk.jfr.Unsigned;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

/**
 *
 * @author smarios <smarios@jaist.ac.jp>
 */
public class IotdbTests implements MqttCallback {

    static final String DBHOST = "127.0.0.1";
    static final int DBPORT = 6667;
    static final String DBNAME = "root.devdb";
    static final String TSNAME = "root.devdb.loc1.src2.temperature";
    static final String DEVICENAME = "root.devdb.loc1.src2";

    static final String MQTTSERVER = "tcp://150.65.179.250";
    static final String MQTTTOPIC = "/+/+/CO2";

    private Session dbsession;
    private MqttClient mqttclient;

    private final List messages;

    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException, MqttException {
        System.out.println("Hello World!");

        IotdbTests tests = new IotdbTests();
        tests.init();
        tests.mainloop();

    }

    public IotdbTests() {
        this.messages = Collections.synchronizedList(new ArrayList<>());
    }

    public void init() throws IoTDBConnectionException, MqttException, StatementExecutionException {
        initializeIotDb();
        initializeMqttClient();
    }

    private void initializeIotDb() throws IoTDBConnectionException, StatementExecutionException {
        dbsession = new Session.Builder()
            .host(DBHOST)
            .port(DBPORT)
            .build();
        dbsession.open();

        if (!dbsession.checkTimeseriesExists(TSNAME)) {
            dbsession.createTimeseries(TSNAME, TSDataType.FLOAT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
        }
    }

    private void initializeMqttClient() throws MqttException {
        mqttclient = new MqttClient(MQTTSERVER, "7777");
        mqttclient.setCallback(this);
        mqttclient.connect();
        mqttclient.subscribe(MQTTTOPIC, 0);
    }

    
    private void mainloop() {
        while (true) {
            handleIncomingMessages();
        }
    }

    private void handleIncomingMessages() {
        String message = getMessage();
        float value = convertMessage(message);
        postToDB(value);
    }

    private String getMessage() {
        synchronized (messages) {
            while (messages.isEmpty()) {
                try {
                    messages.wait();
                } catch (InterruptedException ex) {
                    Logger.getLogger(IotdbTests.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            //we have a message here, deque it
            return (String) messages.remove(0);
        }
    }

    private float convertMessage(String message) {
        return Float.parseFloat(message);
    }

    private void postToDB(float value) {
        //let's put something in there!
        List<String> measurements = new ArrayList<>(List.of("temperature"));
        List<TSDataType> types = new ArrayList<>(List.of(TSDataType.FLOAT));
        List<Object> values = new ArrayList<>(List.of(value));
        Date date = new Date();
        try {
            dbsession.insertRecord(DEVICENAME, date.getTime(), measurements, types, values);
            System.out.println("sent to db: " + values.get(0));
        } catch (IoTDBConnectionException ex) {
            Logger.getLogger(IotdbTests.class.getName()).log(Level.SEVERE, null, ex);
        } catch (StatementExecutionException ex) {
            Logger.getLogger(IotdbTests.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void postInQue(MqttMessage message) {
        synchronized (messages) {
            messages.add(message.toString());
            messages.notify();
        }
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        System.out.println("received message: " + message.toString() + " in topic: " + topic);
        postInQue(message);
    }

    @Override
    public void disconnected(MqttDisconnectResponse disconnectResponse) {
        System.out.println("Disconnected!");
    }

    @Override
    public void mqttErrorOccurred(MqttException exception) {
        System.err.println("exception occurred!" + exception.toString());
    }

    @Override
    public void deliveryComplete(IMqttToken token) {

        System.out.println("something sent!");
    }

    @Override
    public void connectComplete(boolean reconnect, String serverURI) {
        System.out.println("Connected to MQTT server");
    }

    @Override
    public void authPacketArrived(int reasonCode, MqttProperties properties) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
//    private void doDbTest() throws StatementExecutionException, IoTDBConnectionException {
//        try (Session session = new Session.Builder()
//            .host(DBHOST)
//            .port(DBPORT)
//            .build()) {
//            session.open();
//
//            //if (TODO check if database exists, or try to create it and fail, that's good too) {
//            //    session.setStorageGroup(DBNAME);
//            //}
//            if (!session.checkTimeseriesExists(TSNAME)) {
//                session.createTimeseries(TSNAME, TSDataType.FLOAT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
//            }
//
//            //let's put something in there!
//            List<String> measurements = new ArrayList<>(List.of("temperature"));
//            List<TSDataType> types = new ArrayList<>(List.of(TSDataType.FLOAT));
//            List<Object> values = new ArrayList<>(List.of(33.3f));
//            Date date = new Date();
//            session.insertRecord(DEVICENAME, date.getTime(), measurements, types, values);
//        }
//    }
}
