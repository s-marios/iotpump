
package jaist.iotdb.tests;

import org.eclipse.paho.mqttv5.common.MqttMessage;

/**
 *
 * @author smarios <smarios@jaist.ac.jp>
 */
public class TopicAndMessage {
    public final MqttMessage message;
    public final String topic;

    public TopicAndMessage(String topic, MqttMessage message) {
        this.topic = topic;
        this.message = message;
    }
}
