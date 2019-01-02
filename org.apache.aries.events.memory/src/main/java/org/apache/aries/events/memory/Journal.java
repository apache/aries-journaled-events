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
