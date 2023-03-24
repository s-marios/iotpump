package jaist.pump;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import static java.util.Map.entry;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptionsBuilder;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

/**
 *
 * @author smarios <smarios@jaist.ac.jp>
 */
public class Pump implements MqttCallback {

    static final String DBHOST_KEY = "DBHOST";
    static final String DBPORT_KEY = "DBPORT";
    static final String DBNAME_KEY = "DBNAME";

    static final String MQTTSERVER_KEY = "MQTTSERVER";
    static final String MQTTPORT_KEY = "MQTTPORT";
    static final String MQTTTOPICS_KEY = "MQTTTOPICS";

    private Session dbsession;
    private MqttClient mqttclient;

    private final List<TopicAndMessage> messages;
    private final Map<String, DataConvertor> conversions;

    final String dbhost;
    final int dbport;
    final String dbname;
    final String mqttServerUri;
    final int mqttport;
    String[] topics;

    public static class Builder {

        private String dbname = "root.devdb";
        private String dbhost = "127.0.0.1";
        private int dbport = 6667;
        private String mqttServerUri = "tcp://localhost";
        private int mqttPort = 1883;
        private String topics = "/+/+/CO2, /+/+/temperature, /+/+/humidity";

        public Builder() {
        }

        public Builder dbname(String dbname) {
            this.dbname = dbname;
            return this;
        }

        public Builder dbhost(String host) {
            this.dbhost = host;
            return this;
        }

        public Builder dbport(int port) {
            this.dbport = port;
            return this;
        }

        public Builder mqttServerUri(String serverUri) {
            this.mqttServerUri = serverUri;
            return this;
        }

        public Builder mqttPort(int port) {
            this.mqttPort = port;
            return this;
        }

        public Builder topics(String topics) {
            this.topics = topics;
            return this;
        }

        public Pump build() {
            //this is horrible, the use of java arrays
            //when comming from rust it'd be
            //topics.split.map(strip).collect() and go to town
            String[] splits = topics.split(", ");
            String[] scrubbed_topics = new String[splits.length];
            for (int i = 0; i < splits.length; i++) {
                scrubbed_topics[i] = splits[i].strip();
            }

            return new Pump(dbhost, dbport, dbname, mqttServerUri, mqttPort, scrubbed_topics);
        }

        public Pump fromProperties(Properties properties) {

            this.dbhost(properties.getProperty(DBHOST_KEY, dbhost));
            this.dbname(properties.getProperty(DBNAME_KEY, dbname));

            this.mqttServerUri(properties.getProperty(MQTTSERVER_KEY, mqttServerUri));
            this.topics(properties.getProperty(MQTTTOPICS_KEY, topics));

            try {
                this.dbport(Integer.parseInt(properties.getProperty(DBPORT_KEY)));
            } catch (NumberFormatException | NullPointerException ex) {
                Logger.getLogger(Builder.class.getName()).log(Level.WARNING, "malformed or non-existing configuration: DBPORT", ex);
            }

            try {
                this.mqttPort(Integer.parseInt(properties.getProperty(MQTTPORT_KEY)));
            } catch (NumberFormatException | NullPointerException ex) {
                Logger.getLogger(Builder.class.getName()).log(Level.WARNING, "malformed or non-existing configuration: MQTTPORT", ex);
            }

            return this.build();
        }
    }

    public Pump(String dbhost, int dbport, String dbname, String mqttServerUri, int mqttPort, String[] topics) {
        this.dbhost = dbhost;
        this.dbport = dbport;
        this.dbname = dbname;
        this.mqttServerUri = mqttServerUri;
        this.mqttport = mqttPort;
        this.topics = topics;

        this.messages = Collections.synchronizedList(new ArrayList<>());
        this.conversions = Map.ofEntries(
            entry("temperature", DataConvertor.Float()),
            entry("humidity", DataConvertor.Float()),
            entry("co2", DataConvertor.Float())
        );
    }

    Pump() {
        //we just pass defaults to the full constructor
        this(
            "127.0.0.1",
            6667,
            "root.devdb",
            "tcp://127.0.0.1",
            1883,
            new String[]{
                "/+/+/CO2",
                "/+/+/temperature",
                "/+/+/humidity"
            });
    }

    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException, MqttException, IOException {
        System.out.println("Starting IoT Pump!");

        Properties properties = new Properties();
        properties.load(new FileInputStream("config.properties"));
        Pump pump = new Pump.Builder().fromProperties(properties);
        pump.init();

        pump.mainloop();
    }

    public void init() throws IoTDBConnectionException, MqttException, StatementExecutionException {
        connectToIotDb();
        startMqttClient();
    }

    private void connectToIotDb() throws IoTDBConnectionException, StatementExecutionException {

        dbsession = new Session.Builder()
            .host(this.dbhost)
            .port(this.dbport)
            .build();
        dbsession.open();
    }

    private void startMqttClient() throws MqttException {

        mqttclient = new MqttClient(this.mqttServerUri, "iotpump-persistence");
        mqttclient.setCallback(this);

        MqttConnectionOptions options = new MqttConnectionOptionsBuilder()
            .cleanStart(true)
            .automaticReconnect(true)
            .build();

        mqttclient.connect(options);

        for (String topic : topics) {
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
                    Logger.getLogger(Pump.class.getName()).log(Level.SEVERE, null, ex);
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
        return this.dbname + topic.replace('/', '.');
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
            Logger.getLogger(Pump.class.getName()).log(Level.SEVERE, null, ex);
        } catch (StatementExecutionException ex) {
            Logger.getLogger(Pump.class.getName()).log(Level.SEVERE, null, ex);
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
}
