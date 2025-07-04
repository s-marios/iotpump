package jaist.pump;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;

/**
 *
 * @author smarios <smarios@jaist.ac.jp>
 */
public abstract class DataConvertor {

    protected TSDataType type;

    public TSDataType getPrimitiveType() {
        return this.type;
    }

    public DataConvertor(TSDataType type) {
        this.type = type;
    }

    public abstract Object parseValue(String value_str) throws IllegalArgumentException;

    public static DataConvertor Boolean() {
        return new DataConvertor.AsBoolean();
    }

    public static DataConvertor Float() {
        return new DataConvertor.AsFloat();
    }

    public static DataConvertor Double() {
        return new DataConvertor.AsDouble();
    }

    public static DataConvertor Int32() {
        return new DataConvertor.AsInt32();
    }

    public static DataConvertor Text() {
        return new DataConvertor.AsText();
    }

    public static DataConvertor DoubleOrText() {
        return new DataConvertor.AsDoubleOrText();
    }

    public static class AsBoolean extends DataConvertor {

        public AsBoolean() {
            super(TSDataType.BOOLEAN);
        }

        @Override
        public Object parseValue(String value_str) {
            String value_lower = value_str.toLowerCase().strip();
            switch (value_lower) {
                case "yes":
                case "true":
                case "1":
                    return true;
                case "no":
                case "false":
                case "0":
                    return false;
                default:
                    throw new IllegalArgumentException("Unable to parse " + value_str + " as a boolean");
            }
        }
    }

    public static class AsFloat extends DataConvertor {

        public AsFloat() {
            super(TSDataType.FLOAT);
        }

        @Override
        public Object parseValue(String value_str) throws IllegalArgumentException {
            return Float.valueOf(value_str);
        }
    }

    public static class AsDouble extends DataConvertor {

        public AsDouble() {
            super(TSDataType.DOUBLE);
        }

        @Override
        public Object parseValue(String value_str) throws IllegalArgumentException {
            return Double.valueOf(value_str);
        }
    }

    public static class AsInt32 extends DataConvertor {

        public AsInt32() {
            super(TSDataType.INT32);
        }

        @Override
        public Object parseValue(String value_str) throws IllegalArgumentException {
            return Math.round(Float.parseFloat(value_str));
        }

    }

    public static class AsText extends DataConvertor {

        public AsText() {
            super(TSDataType.TEXT);
        }

        @Override
        public Object parseValue(String value_str) throws IllegalArgumentException {
            return new Binary(value_str.getBytes());
        }
    }

    public static class AsDoubleOrText extends DataConvertor {

        public AsDoubleOrText() {
            super(TSDataType.DOUBLE);
        }

        @Override
        public Object parseValue(String value_str) throws IllegalArgumentException {
            try {
                this.type = TSDataType.DOUBLE;
                return Double.valueOf(value_str);
            } catch (NumberFormatException e) {
                this.type = TSDataType.TEXT;
                return new Binary(value_str.getBytes());
            }
        }

    }
}
