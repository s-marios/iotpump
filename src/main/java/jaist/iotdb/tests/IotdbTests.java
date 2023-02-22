package jaist.iotdb.tests;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import static java.util.Map.entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
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

    static final String MQTTSERVER = "tcp://150.65.179.250";
    static final String[] MQTTTOPICS = {
        "/+/+/CO2",
        "/+/+/temperature",
        "/+/+/humidity"
    };

    private Session dbsession;
    private MqttClient mqttclient;

    private final List<TopicAndMessage> messages;

    private final Map<String, DataConvertor> conversions;

    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException, MqttException {
        System.out.println("Hello World!");

        IotdbTests tests = new IotdbTests();
        tests.init();
        tests.mainloop();

    }

    public IotdbTests() {
        this.messages = Collections.synchronizedList(new ArrayList<>());
        this.conversions = Map.ofEntries(
            entry("temperature", DataConvertor.Float()),
            entry("humidity", DataConvertor.Float()),
            entry("co2", DataConvertor.Float())
        );
    }

    public void init() throws IoTDBConnectionException, MqttException, StatementExecutionException {
        connectToIotDb();
        startMqttClient();
    }

    private void connectToIotDb() throws IoTDBConnectionException, StatementExecutionException {
        dbsession = new Session.Builder()
            .host(DBHOST)
            .port(DBPORT)
            .build();
        dbsession.open();
    }

    private void startMqttClient() throws MqttException {
        mqttclient = new MqttClient(MQTTSERVER, "7777");
        mqttclient.setCallback(this);
        mqttclient.connect();
        for (String topic : MQTTTOPICS) {
            mqttclient.subscribe(topic, 0);
        }
    }

    private void mainloop() {
        while (true) {
            handleIncomingMessages();
        }
    }

    private void handleIncomingMessages() {
        TopicAndMessage tm = getMessage();
        TimeSeriesAndValue tsval = convertMessage(tm);
        postToDB(tsval);
    }

    private TopicAndMessage getMessage() {
        synchronized (messages) {
            while (messages.isEmpty()) {
                try {
                    messages.wait();
                } catch (InterruptedException ex) {
                    Logger.getLogger(IotdbTests.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            //we have a message here, deque it
            return (TopicAndMessage) messages.remove(0);
        }
    }

    private TimeSeriesAndValue convertMessage(TopicAndMessage message) {
        String timeseries = convertTopicToTimeseries(message.topic);
        String datatype = getTopicSuffix(message.topic).toLowerCase();
        DataConvertor convertor = conversions.get(datatype);
        Object parseValue = convertor.parseValue(message.message.toString());
        return new TimeSeriesAndValue(timeseries, convertor.getPrimitiveType(), parseValue);
    }

    private String convertTopicToTimeseries(String topic) {
        return DBNAME + topic.replace('/', '.');
    }

    private String getTopicSuffix(String mqtttopic) {
        return mqtttopic.substring(mqtttopic.lastIndexOf('/') + 1);
    }

    private <T> void postToDB(TimeSeriesAndValue tsval) {
        List<String> measurements = new ArrayList<>(List.of(tsval.getSuffix()));
        List<TSDataType> types = new ArrayList<>(List.of(tsval.getDataType()));
        List<Object> values = new ArrayList<>(List.of(tsval.getValue()));
        Date date = new Date();
        try {
            dbsession.insertRecord(tsval.getPrefix(), date.getTime(), measurements, types, values);
            System.out.println("Posted in: " + tsval.timeseries + " value: " + values.get(0));
        } catch (IoTDBConnectionException ex) {
            Logger.getLogger(IotdbTests.class.getName()).log(Level.SEVERE, null, ex);
        } catch (StatementExecutionException ex) {
            Logger.getLogger(IotdbTests.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void postInQue(TopicAndMessage message) {
        synchronized (messages) {
            messages.add(message);
            messages.notify();
        }
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        System.out.println("received message: " + message.toString() + " in topic: " + topic);
        postInQue(new TopicAndMessage(topic, message));
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
