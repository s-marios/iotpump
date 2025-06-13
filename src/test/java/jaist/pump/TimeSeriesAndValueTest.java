package jaist.pump;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author smarios <smarios@jaist.ac.jp>
 */
public class TimeSeriesAndValueTest {

    public TimeSeriesAndValueTest() {
    }

    @BeforeAll
    public static void setUpClass() {
    }

    @AfterAll
    public static void tearDownClass() {
    }

    @BeforeEach
    public void setUp() {
    }

    @AfterEach
    public void tearDown() {
    }

    /**
     * Test of getDataType method, of class TimeSeriesAndValue.
     */
    @org.junit.jupiter.api.Test
    public void testGetDataTypeForText() {
        System.out.println("getDataType");
        TimeSeriesAndValue instance = new TimeSeriesAndValue("ignore", TSDataType.TEXT, new Binary("text".getBytes()));
        TSDataType expResult = TSDataType.TEXT;
        TSDataType result = instance.getDataType();
        assertEquals(expResult, result);
        assertEquals(new Binary("text".getBytes()), instance.getValue());
    }

    /**
     * Test of getValue method, of class TimeSeriesAndValue.
     */
    @org.junit.jupiter.api.Test
    public void testGetValue() {
        System.out.println("getValue");
        TimeSeriesAndValue instance = new TimeSeriesAndValue("ignore", TSDataType.FLOAT, 1.3f);
        Object expResult = 1.3f;
        Object result = instance.getValue();
        assertEquals(expResult, result);
    }

    /**
     * Test of getSuffix method, of class TimeSeriesAndValue.
     */
    @org.junit.jupiter.api.Test
    public void testGetSuffix() {
        System.out.println("getSuffix");
        TimeSeriesAndValue instance = new TimeSeriesAndValue("root.somewhere.src1.temperature", TSDataType.TEXT, new Binary("ignore".getBytes()));
        String expResult = "temperature";
        String result = instance.getSuffix();
        assertEquals(expResult, result);
    }

    /**
     * Test of getPrefix method, of class TimeSeriesAndValue.
     */
    @org.junit.jupiter.api.Test
    public void testGetPrefix() {
        System.out.println("getPrefix");
        TimeSeriesAndValue instance = new TimeSeriesAndValue("root.somewhere.src1.temperature", TSDataType.TEXT, new Binary("ignore".getBytes()));
        String expResult = "root.somewhere.src1";
        String result = instance.getPrefix();
        assertEquals(expResult, result);
    }

}
