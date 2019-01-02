package org.apache.aries.events.memory;

import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class Journal<T> {
    private AtomicLong nextOffset = new AtomicLong();
    private ConcurrentNavigableMap<Long, T> messages = new ConcurrentSkipListMap<>();
    
    public long append(T message) {
        Long offset = nextOffset.getAndIncrement();
        messages.put(offset, message);
        return offset;
    }

    public long getFirstOffset() {
        try {
            return messages.firstKey();
        } catch (NoSuchElementException e) {
            return 0;
        }
    }

    public long getLastOffset() {
        try {
            return messages.lastKey();
        } catch (NoSuchElementException e) {
            return -1;
        }
    }

    public Entry<Long, T> getNext(long offset) {
        return this.messages.ceilingEntry(offset);
    }
}
