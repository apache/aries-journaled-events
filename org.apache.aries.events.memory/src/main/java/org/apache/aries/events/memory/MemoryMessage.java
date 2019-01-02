package org.apache.aries.events.memory;

import java.util.Map;

import org.apache.aries.events.api.Message;

class MemoryMessage implements Message {

    private byte[] payload;
    private Map<String, String> properties;

    MemoryMessage(byte[] payload, Map<String, String> props) {
        this.payload = payload;
        properties = props;
    }

    @Override
    public byte[] getPayload() {
        return this.payload;
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

}
