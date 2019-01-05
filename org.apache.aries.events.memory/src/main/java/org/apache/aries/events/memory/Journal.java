/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The SF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.aries.events.memory;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

class Journal<T> {
    private final int keepAtLeast;
    private final AtomicLong nextOffset = new AtomicLong();
    private final ConcurrentNavigableMap<Long, T> messages = new ConcurrentSkipListMap<>();
    private final AtomicLong count = new AtomicLong();
    
    public Journal(int keepAtLeast) {
        this.keepAtLeast = keepAtLeast;
    }
    
    public long append(T message) {
        if (count.incrementAndGet() > keepAtLeast * 2) {
            evict();
        }
        Long offset = nextOffset.getAndIncrement();
        messages.put(offset, message);
        return offset;
    }

    private synchronized void evict() {
        Iterator<Long> it = messages.keySet().iterator();
        for (int c = 0; c < keepAtLeast; c++) {
            messages.remove(it.next());
        }
        count.set(0);
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
