package org.apache.aries.events.api;

import java.util.Map;

/**
 * TODO If we allow wild card consumption then a message also needs a topic
 */
public interface Message {
    byte[] getPayload();
    
    /**
     * Position of the message in the topic
     * @return
     */
    Position getPosition();
    Map<String, String> getProperties();
}
