package org.apache.aries.events.api;

import java.util.Map;
import java.util.function.Consumer;

/**
 * Journaled messaging API
 */
public interface Messaging {
    /**
     * Send a message to a topic. When this method returns the message 
     * is safely persisted.
     */
    Position send(String topic, Message message);

    /**
     * Subscribe to a topic. The callback is called for each message received.
     * 
     * @param topic to consume from. TODO Do we allow wild cards? 
     * @param position in the topic to start consuming from 
     * @param seek where to start from when position is not valid or null
     * @param callback will be called for each message received
     * @return Returned subscription must be closed by the caller to unsubscribe
     */
    Subscription subscribe(String topic, Position position, Seek seek, Consumer<Received> callback);

    /**
     * Create a message with payload and metadata
     * @param payload
     * @param props
     * @return
     */
    Message newMessage(byte[] payload, Map<String, String> props);

    /**
     * Deserialize the position from the string
     * 
     * @param position
     * @return
     */
    Position positionFromString(String position);

}
