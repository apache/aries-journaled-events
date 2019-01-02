package org.apache.aries.events.memory;

import org.apache.aries.events.api.Position;

class MemoryPosition implements Position {

    private long offset;

    MemoryPosition(long offset) {
        this.offset = offset;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return new Long(offset).toString();
    }
}
