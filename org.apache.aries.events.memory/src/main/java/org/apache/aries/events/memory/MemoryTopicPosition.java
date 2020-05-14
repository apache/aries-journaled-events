package org.apache.aries.events.memory;

import org.apache.aries.events.api.TopicPosition;

public class MemoryTopicPosition implements TopicPosition {

    private long offset;
    
    public MemoryTopicPosition(long offset) {
        this.offset = offset;
    }

    @Override
    public String topicPositionToString() {
            return Long.toString(offset);
    }

    public long getOffset() {
        return offset;
    }

}
