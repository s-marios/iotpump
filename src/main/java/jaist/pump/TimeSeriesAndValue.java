package jaist.pump;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.TsPrimitiveType;

/**
 *
 * @author smarios <smarios@jaist.ac.jp>
 */
public class TimeSeriesAndValue {

    public String timeseries;
    public TsPrimitiveType value;

    /**
     *
     * @param timeseries we assume that this is a well-formatted timeseries as a
     * string. Its last part is a suffix, and anything before it is a prefix
     * @param type
     * @param value
     */
    public TimeSeriesAndValue(String timeseries, TSDataType type, Object value) {
        this.timeseries = timeseries;
        this.value = TsPrimitiveType.getByType(type, value);
    }

    public TSDataType getDataType() {
        return this.value.getDataType();
    }

    public Object getValue() {
        return this.value.getValue();
    }

    public String getSuffix() {
        return timeseries.substring(timeseries.lastIndexOf('.') + 1);
    }

    public String getPrefix() {
        return timeseries.substring(0, timeseries.lastIndexOf('.'));
    }

}
