package jaist.pump;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 *
 * @author smarios <smarios@jaist.ac.jp>
 */
public class DataConvertorTest {

    public DataConvertorTest() {
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
     * Test of getPrimitiveType method, of class DataConvertor.
     */
    @Test
    public void testGetPrimitiveType() {
        System.out.println("getPrimitiveType");
        DataConvertor instance = new DataConvertor.AsBoolean();
        TSDataType expResult = TSDataType.BOOLEAN;
        TSDataType result = instance.getPrimitiveType();
        assertEquals(expResult, result);
    }

    /**
     * Test of parseValue method, of class DataConvertor.
     */
    @Test
    public void testParseValue() {
        System.out.println("parseValue");
        String value_str = "test string";
        DataConvertor instance = new DataConvertor.AsText();
        Object expResult = new Binary("test string".getBytes());
        Object result = instance.parseValue(value_str);
        assertEquals(expResult, result);
    }

    /**
     * Test of Boolean method, of class DataConvertor.
     */
    @Test
    public void testBoolean() {
        System.out.println("Boolean");
        DataConvertor expResult = new DataConvertor.AsBoolean();
        DataConvertor result = DataConvertor.Boolean();
        assertEquals(expResult.getClass(), result.getClass());

    }

    /**
     * Test of Float method, of class DataConvertor.
     */
    @Test
    public void testFloat() {
        System.out.println("Float");
        DataConvertor expResult = new DataConvertor.AsFloat();
        DataConvertor result = DataConvertor.Float();
        assertEquals(expResult.getClass(), result.getClass());
    }

    /**
     * Test of Int32 method, of class DataConvertor.
     */
    @Test
    public void testInt32() {
        System.out.println("Int32");
        DataConvertor expResult = new DataConvertor.AsInt32();
        DataConvertor result = DataConvertor.Int32();
        assertEquals(expResult.getClass(), result.getClass());
    }

    @Test
    public void testInt32parsesFloatString() {
        DataConvertor dc = DataConvertor.Int32();
        Object parsed = dc.parseValue("1.00");
        assertEquals(1, parsed);
        parsed = dc.parseValue("1.01");
        assertEquals(1, parsed);
        parsed = dc.parseValue("0.99");
        assertEquals(1, parsed);

    }

    /**
     * Test of Text method, of class DataConvertor.
     */
    @Test
    public void testText() {
        System.out.println("Text");
        DataConvertor expResult = new DataConvertor.AsText();
        DataConvertor result = DataConvertor.Text();
        assertEquals(expResult.getClass(), result.getClass());
    }

}
