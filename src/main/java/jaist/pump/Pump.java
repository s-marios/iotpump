package jaist.pump;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.tsfile.enums.TSDataType;
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
    static final String DBUSERNAME_KEY = "DBUSERNAME";
    static final String DBPASSWORD_KEY = "DBPASSWORD";

    static final String MQTTSERVER_KEY = "MQTTSERVER";
    static final String MQTTPORT_KEY = "MQTTPORT";
    static final String MQTTTOPICS_KEY = "MQTTTOPICS";
    static final String MQTTCLIENTID_KEY = "MQTTCLIENTID";

    static final String CONV_BOOL_KEY = "CONVERT-BOOL";
    static final String CONV_INT_KEY = "CONVERT-INT";
    static final String CONV_FLOAT_KEY = "CONVERT-FLOAT";
    static final String CONV_DOUBLE_KEY = "CONVERT-DOUBLE";
    static final String CONV_TEXT_KEY = "CONVERT-TEXT";

    private Session dbsession;
    private MqttClient mqttclient;

    private final List<TopicAndMessage> messages;

    final String dbhost;
    final int dbport;
    final String dbname;
    final String mqttServerUri;
    final int mqttport;
    String[] topics;
    private final String dbusername;
    private final String dbpassword;
    final String mqttClientId;
    private final Map<String, DataConvertor> conversions;
    private final DataConvertor defaultConvertor = DataConvertor.DoubleOrText();

    public static class Builder {

        private String dbname = "root.devdb";
        private String dbhost = "127.0.0.1";
        private int dbport = 6667;
        private String dbusername = "root";
        private String dbpassword = "root";
        private String mqttServerUri = "tcp://localhost";
        private int mqttPort = 1883;
        private String topics = null;
        private String mqttClientId = "iotpump-persistence";
        private final Map<String, DataConvertor> conversions = new HashMap<>();

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

        public Builder dbusername(String username) {
            this.dbusername = username;
            return this;
        }

        public Builder dbpassword(String password) {
            this.dbpassword = password;
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

        public Builder mqttClientId(String id) {
            this.mqttClientId = id;
            return this;
        }

        public Pump build() {
            if (this.topics == null) {
                throw new IllegalArgumentException("no valid topics configuration!");
            }
            //this is horrible, the use of java arrays
            //when comming from rust it'd be
            //topics.split.map(strip).collect() and go to town
            String[] splits = topics.split(",");
            String[] scrubbed_topics = new String[splits.length];
            for (int i = 0; i < splits.length; i++) {
                scrubbed_topics[i] = splits[i].strip();
            }

            return new Pump(dbhost, dbport, dbusername, dbpassword, dbname, mqttServerUri, mqttPort, scrubbed_topics, mqttClientId, conversions);
        }

        private void loadConvertor(Properties properties, String key, DataConvertor convertor) {
            var prop = properties.getProperty(key);
            if (prop == null) {
                return;
            }

            String[] splits = prop.split(",");
            for (var split : splits) {
                conversions.put(split.strip().toLowerCase(), convertor);
            }

        }

        private Map<String, DataConvertor> loadConvertors(Properties properties) {
            loadConvertor(properties, CONV_FLOAT_KEY, DataConvertor.Float());
            loadConvertor(properties, CONV_DOUBLE_KEY, DataConvertor.Double());
            loadConvertor(properties, CONV_INT_KEY, DataConvertor.Int32());
            loadConvertor(properties, CONV_TEXT_KEY, DataConvertor.Text());
            loadConvertor(properties, CONV_BOOL_KEY, DataConvertor.Boolean());
            return this.conversions;
        }

        public Pump fromProperties(Properties properties) {

            this.dbhost(properties.getProperty(DBHOST_KEY, dbhost));
            this.dbname(properties.getProperty(DBNAME_KEY, dbname));
            this.dbusername(properties.getProperty(DBUSERNAME_KEY, dbusername));
            this.dbpassword(properties.getProperty(DBPASSWORD_KEY, dbpassword));

            this.mqttServerUri(properties.getProperty(MQTTSERVER_KEY, mqttServerUri));
            this.topics(properties.getProperty(MQTTTOPICS_KEY, topics));
            this.mqttClientId(properties.getProperty(MQTTCLIENTID_KEY, this.mqttClientId));

            try {
                this.dbport(Integer.parseInt(properties.getProperty(DBPORT_KEY)));
            } catch (NumberFormatException | NullPointerException ex) {
                Logger.getLogger(Builder.class.getName()).log(Level.WARNING, "malformed or non-existing configuration: DBPORT, using default port: " + this.dbport, ex);
            }

            try {
                this.mqttPort(Integer.parseInt(properties.getProperty(MQTTPORT_KEY)));
            } catch (NumberFormatException | NullPointerException ex) {
                Logger.getLogger(Builder.class.getName()).log(Level.WARNING, "malformed or non-existing configuration: MQTTPORT, using default port: " + this.mqttPort, ex);
            }

            this.loadConvertors(properties);

            return this.build();
        }
    }

    public Pump(String dbhost, int dbport, String dbusername, String dbpassword, String dbname, String mqttServerUri, int mqttPort, String[] topics, String mqttClientId, Map<String, DataConvertor> conversions) {
        this.dbhost = dbhost;
        this.dbport = dbport;
        this.dbusername = dbusername;
        this.dbpassword = dbpassword;
        this.dbname = dbname;
        this.mqttServerUri = mqttServerUri;
        this.mqttport = mqttPort;
        this.topics = topics;
        this.mqttClientId = mqttClientId;
        this.conversions = conversions;

        this.messages = Collections.synchronizedList(new ArrayList<>());

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
            .username(this.dbusername)
            .password(this.dbpassword)
            .build();
        dbsession.open();
    }

    private void startMqttClient() throws MqttException {

        mqttclient = new MqttClient(this.mqttServerUri, this.mqttClientId);
        mqttclient.setCallback(this);

        MqttConnectionOptions options = new MqttConnectionOptionsBuilder()
            .cleanStart(true)
            .automaticReconnect(true)
            .build();

        mqttclient.connect(options);

    }

    private void mainloop() {
        while (true) {
            handleIncomingMessages();
        }
    }

    private void handleIncomingMessages() {
        TopicAndMessage tm = getMessage();
        TimeSeriesAndValue tsval = convertMessage(tm);
        if (tsval != null) {
            postToDB(tsval);
        }
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
        DataConvertor convertor = getConvertor(getTopicSuffix(message.topic));
        try {
            Object parseValue = convertor.parseValue(message.message.toString());
            return new TimeSeriesAndValue(timeseries, convertor.getPrimitiveType(), parseValue);
        } catch (IllegalArgumentException ex) {
            Logger.getLogger(Pump.class.getName()).log(
                Level.WARNING, "failed to convert value: " + message.message.toString(), ex);
            return null;
        }
    }

    //package private for testing
    DataConvertor getConvertor(String key) {
        return conversions.getOrDefault(key.toLowerCase(), defaultConvertor);
    }

    //we convert an MQTT topic by
    // 1. replacing any dots with undrescores (dots add an extra level in an IoTDB dataseries
    // 2. replacing MQTT level separators ('/') with IoTDB level separators ('.')
    // 3. prepend the configured db/timeseries prefix
    //package private just for testing
    String convertTopicToTimeseries(String topic) {
        if (topic.startsWith("/") == false) {
            topic = "/" + topic;
        }
        return this.dbname + topic.replace('.', '_').replace('/', '.');
    }

    //package private just for testing
    String getTopicSuffix(String mqtttopic) {
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
        } catch (IoTDBConnectionException | StatementExecutionException ex) {
            Logger.getLogger(Pump.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void postInQue(TopicAndMessage message) {
        synchronized (messages) {
            messages.add(message);
            messages.notify();
        }
    }

    private void subscribeToTopics() {
        for (String topic : topics) {
            try {
                mqttclient.subscribe(topic, 0);
            } catch (MqttException ex) {
                //if we fail to subscribe, catch fire and die
                Logger.getLogger(Pump.class.getName()).log(Level.SEVERE, null, ex);
                System.exit(-1);
            }
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
        subscribeToTopics();
    }

    @Override
    public void authPacketArrived(int reasonCode, MqttProperties properties) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
