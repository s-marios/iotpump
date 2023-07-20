package jaist.pump;

import java.util.Map;
import static java.util.Map.entry;

/**
 * Register data convertors for various types of information.
 *
 * We expect one of the levels in an MQTT topic contains the type of the
 * information of that topic. Register data convertors using *lower case*
 * matching the information type.
 *
 * @author smarios <smarios@jaist.ac.jp>
 */
public class Conversions {

    private static final Map<String, DataConvertor> conversions = Map.ofEntries(
        entry("temperature", DataConvertor.Float()),
        entry("humidity", DataConvertor.Float()),
        entry("co2", DataConvertor.Float()),
        entry("voc", DataConvertor.Float()),
        entry("nox", DataConvertor.Float()),
        entry("pm1", DataConvertor.Float()),
        entry("pm2.5", DataConvertor.Float()),
        entry("pm4", DataConvertor.Float()),
        entry("pm10", DataConvertor.Float()),
        entry("lux", DataConvertor.Float()),
        entry("presence", DataConvertor.Int32()),
        entry("button", DataConvertor.Int32())
    );

    public static DataConvertor get(String datatype) {
        return conversions.get(datatype.toLowerCase());
    }
}
