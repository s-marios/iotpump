package jaist.pump;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author smarios <smarios@jaist.ac.jp>
 */
public class PumpTest {

    public PumpTest() {

    }

    @Test
    public void testLoadProperties() {
        Properties props = new Properties();

        try {
            props.load(new FileInputStream("config_example.properties"));
            assertEquals("6667", props.getProperty("DBPORT"));
            assertEquals("tcp://127.0.0.1", props.getProperty("MQTTSERVER"));
            assertEquals("/+/+/CO2, /+/+/temperature,  /+/+/humidity", props.getProperty("MQTTTOPICS"));
        } catch (IOException ex) {
            Logger.getLogger(PumpTest.class.getName()).log(Level.SEVERE, null, ex);
            fail();
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

        String[] expectedTopics = new String[]{
            "/+/+/CO2", "/+/+/temperature", "/+/+/humidity"};

        for (int i = 0; i < pump.topics.length; i++) {
            assertEquals(expectedTopics[i], pump.topics[i]);
        }
    }

}
