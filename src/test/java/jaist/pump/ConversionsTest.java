package jaist.pump;

import org.apache.tsfile.enums.TSDataType;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

/**
 *
 * @author smarios <smarios@jaist.ac.jp>
 */
public class ConversionsTest {

    public ConversionsTest() {
    }

    @Test
    public void getPM2dot5Succeeds() {
        DataConvertor dc = Conversions.get("PM2.5");
        assertTrue(dc != null);
        assertEquals(dc.getPrimitiveType(), TSDataType.FLOAT);
    }

}
