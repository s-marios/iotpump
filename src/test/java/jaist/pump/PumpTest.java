package jaist.pump;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

/**
 *
 * @author smarios <smarios@jaist.ac.jp>
 */
public class PumpTest {

    private final Pump uninit_pump;

    private static String default_topics = String.join(",",
        "/+/+/CO2",
        "/+/+/temperature",
        "/+/+/humidity",
        "/+/+/VOC",
        "/+/+/NOx",
        "/+/+/PM1",
        "/+/+/PM2.5",
        "/+/+/PM4",
        "/+/+/PM10",
        "/+/+/lux",
        "/+/+/presence",
        "/+/+/button");

    public PumpTest() throws FileNotFoundException, IOException {
        Properties props = new Properties();
        props.load(new FileInputStream("config_example.properties"));
        uninit_pump = new Pump.Builder().fromProperties(props);
    }

    @Test
    public void testLoadProperties() {
        Properties props = new Properties();

        try {
            props.load(new FileInputStream("config_example.properties"));
            assertEquals("6667", props.getProperty("DBPORT"));
            assertEquals("tcp://127.0.0.1", props.getProperty("MQTTSERVER"));
            assertEquals(default_topics, props.getProperty("MQTTTOPICS"));
        } catch (IOException ex) {
            Logger.getLogger(PumpTest.class.getName()).log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testLoadPropertiesExplicitConvertors() {
        Properties props = new Properties();
        try {
            props.load(new FileInputStream("config_test.properties"));
            var builder = new Pump.Builder();
            Pump pump = builder.fromProperties(props);

            var float_convs = "CO2, temperature, humidity, VOC, NOx, PM1, PM2.5, PM4, PM10, lux";
            for (var key : float_convs.split(",")) {
                assertEquals(DataConvertor.Float().getClass(), pump.getConvertor(key.strip()).getClass());
            }

            var bool_convs = "presence, button";
            for (var key : bool_convs.split(",")) {
                assertEquals(DataConvertor.Boolean().getClass(), pump.getConvertor(key.strip()).getClass());
            }

            assertEquals(DataConvertor.Double().getClass(), pump.getConvertor("todouble").getClass());
            assertEquals(DataConvertor.Text().getClass(), pump.getConvertor("totext").getClass());
            assertEquals(DataConvertor.Int32().getClass(), pump.getConvertor("toint32").getClass());

        } catch (IOException ex) {
            fail("could not open the test configuration file");
        }

    }

    @Test
    public void testBuilderFromProperties() throws IOException {
        Properties props = new Properties();
        props.load(new FileInputStream("config_example.properties"));
        Pump pump = new Pump.Builder().fromProperties(props);

        assertEquals("127.0.0.1", pump.dbhost);
        assertEquals("root.devdb", pump.dbname);
        assertEquals(6667, pump.dbport);
        assertEquals("tcp://127.0.0.1", pump.mqttServerUri);
        assertEquals(1883, pump.mqttport);

        String[] expectedTopics = default_topics.split(",");

        assertEquals(expectedTopics.length, pump.topics.length);

        for (int i = 0; i < pump.topics.length; i++) {
            assertEquals(expectedTopics[i], pump.topics[i]);
        }

        assertEquals("client_id", pump.mqttClientId);
    }

    @Test
    public void getTopicSuffixWorks() throws FileNotFoundException, IOException {
        assertEquals("PM2.5", uninit_pump.getTopicSuffix("/test/topic/PM2.5"));
        assertEquals("a", uninit_pump.getTopicSuffix("/a"));
        assertEquals("", uninit_pump.getTopicSuffix("/"));
    }

    @Test
    public void convertTopicToTimeseriesWithDotsWorks() throws FileNotFoundException, IOException {
        assertEquals("root.devdb.test.topic.PM2_5", uninit_pump.convertTopicToTimeseries("/test/topic/PM2.5"));
        assertEquals("root.devdb.t_e_s_t.t_o_p_i_c.P_M_2_5_", uninit_pump.convertTopicToTimeseries("/t.e.s.t/t.o.p.i.c/P.M.2.5."));
        assertEquals("root.devdb.nostartingslash", uninit_pump.convertTopicToTimeseries("nostartingslash"));
    }

    @Test
    public void NoTopicConfigurationThrows() {
        try {
            new Pump.Builder().build();
            fail("did not throw!");
        } catch (IllegalArgumentException ex) {
            //expected
        }
    }
}
