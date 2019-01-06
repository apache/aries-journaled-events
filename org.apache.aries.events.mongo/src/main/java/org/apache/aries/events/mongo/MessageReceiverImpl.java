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

package org.apache.aries.events.mongo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import org.apache.aries.events.api.Message;
import org.apache.aries.events.api.Received;
import org.apache.aries.events.mongo.Common.Fields;
import org.bson.Document;
import org.bson.types.Binary;
import org.slf4j.Logger;

import static java.lang.Math.min;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static java.util.Collections.emptyList;
import static org.apache.aries.events.mongo.Common.Fields.PAYLOAD;
import static org.apache.aries.events.mongo.Common.Fields.INDEX;
import static org.apache.aries.events.mongo.Common.upcomingIndex;
import static org.slf4j.LoggerFactory.getLogger;

final class MessageReceiverImpl implements MessageReceiver {

    static MessageReceiver messageReceiver(MongoCollection<Document> col) {
        return new MessageReceiverImpl(col, Optional.empty());
    }

    @Override
    public Message receive(long index) throws InterruptedException {
        fetch(index);
        long bufferIndex = index - firstIndex;
        assert bufferIndex < buffer.size() : bufferIndex + ", " + buffer.size();
        return buffer.get((int) bufferIndex);
    }

    @Override
    public long earliestIndex() {
       refreshBuffer(FIRST_AVAILABLE);
       return firstIndex;
    }

    @Override
    public long latestIndex() {
        long result = upcomingIndex(col);
        if (result > 0) {
            result -= 1;
        }
        return result;
    }

    @Override
    public void close() {
        // MongoDB driver doesn't like to be interruped so
        // we try to get out of the poll loop in a gentle way
        interrupted = true;
        mongoClient.ifPresent(Mongo::close);
    }

    //*********************************************
    // Internals
    //*********************************************

    private static final Logger LOGGER = getLogger(MessageReceiverImpl.class);
    private static final long FINE_GRAINED_DELAY = 100L;
    private static final long FIRST_AVAILABLE = -1;
    private final Optional<MongoClient> mongoClient;
    private final MongoCollection<Document> col;
    private long maxWaitTime = 1000L;
    private int fetchLimit = 100;
    private long lastReceived = currentTimeMillis();
    private long firstIndex = 0L;
    private List<Message> buffer = emptyList();
    private volatile boolean interrupted = false;

    private MessageReceiverImpl(MongoCollection<Document> col, Optional<MongoClient> mongoClient) {
        LOGGER.info("Creating new receiver: " + col.getNamespace().getCollectionName());
        this.mongoClient = mongoClient;
        this.col = col;
    }

    private void fetch(long index) throws InterruptedException {
        while (firstIndex > index || firstIndex + buffer.size() <= index) {
            long delay = min(maxWaitTime, (currentTimeMillis() - lastReceived) / 2);
            adaptivePause(delay);
            refreshBuffer(index);
        }
    }

    private void refreshBuffer(long index) {
        long startIndex = index;
        try (MongoCursor<Document> cursor = col.find(Filters.gte(INDEX, startIndex)).iterator()) {
            List<Message> collected = new ArrayList<>(fetchLimit);
            while (cursor.hasNext()) {
                int i = collected.size();
                Document document = cursor.next();
                long idx = document.get(INDEX, Long.class);
                if (startIndex == FIRST_AVAILABLE) {
                    startIndex = idx;
                }
                if (idx == startIndex + i) {
                    Binary payload = document.get(PAYLOAD, Binary.class);
                    Map<String, String> props = (Map<String, String>) document.get(Fields.PROPS);
                    Message message = new MongoMessage(payload.getData(), props);
                    collected.add(message);
                } else {
                    if (i == 0) {
                        throw new NoSuchElementException("Element [" + startIndex + "] has been evicted from the log. Oldest available: [" + idx + "]");
                    } else {
                        throw new IllegalStateException("Missing element at [" + (startIndex + i) + "]. Next available at [" + idx + "]");
                    }
                }
            }
            buffer = collected;
            firstIndex = (startIndex == FIRST_AVAILABLE) ? 0L : startIndex;
            if (collected.size() > 0) {
                lastReceived = currentTimeMillis();
            }
        }
    }

    @SuppressWarnings("BusyWait")
    private void adaptivePause(long ms) throws InterruptedException {
        if (interrupted) {
            throw new InterruptedException();
        }
        long currentTime = currentTimeMillis();
        long stopTime = currentTime + ms;
        while (currentTime < stopTime) {
            if (interrupted) {
                throw new InterruptedException();
            }
            sleep(min(FINE_GRAINED_DELAY, ms));
            currentTime = currentTimeMillis();
        }
    }

}
