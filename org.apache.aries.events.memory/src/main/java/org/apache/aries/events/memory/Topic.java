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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.aries.events.api.Event;
import org.apache.aries.events.api.Position;
import org.apache.aries.events.api.Received;
import org.apache.aries.events.api.Seek;
import org.apache.aries.events.api.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Topic {
    private Logger log = LoggerFactory.getLogger(this.getClass());

    private String topicName;
    private Journal<Event> journal;

    public Topic(String topicName) {
        this.topicName = topicName;
        this.journal = new Journal<>();
    }

    public Position send(Event event) {
        long offset = this.journal.append(event);
        return new MemoryPosition(offset);
    }

    public Subscription subscribe(Position position, Seek seek, Consumer<Received> callback) {
        long startOffset = getStartOffset(position, seek);
        log.debug("Consuming from " + startOffset);
        return new TopicSubscription(startOffset, callback);
    }

    private long getStartOffset(Position position, Seek seek) {
        if (position != null) {
            return position.getOffset();
        } else {
            if (seek == Seek.earliest) {
                return this.journal.getFirstOffset();
            } else if (seek == Seek.latest) {
                return this.journal.getLastOffset() + 1;
            } else {
                throw new IllegalArgumentException("Seek must not be null");
            }
        }
    }

    class TopicSubscription implements Subscription {
        private Consumer<Received> callback;
        private ExecutorService executor;
        private volatile boolean running;
        private long currentOffset;

        TopicSubscription(long startOffset, Consumer<Received> callback) {
            this.currentOffset = startOffset;
            this.callback = callback;
            this.running = true;
            String name = "Poller for " + topicName;
            this.executor = Executors.newSingleThreadExecutor(r -> new Thread(r, name));
            this.executor.execute(this::poll);
        }
        
        private void poll() {
            while (running) {
                Entry<Long, Event> entry = journal.getNext(currentOffset);
                if (entry != null) {
                    long offset = entry.getKey();
                    try {
                        MemoryPosition position = new MemoryPosition(this.currentOffset);
                        Received received = new Received(position, entry.getValue());
                        callback.accept(received);
                    } catch (Exception e) {
                        log.warn(e.getMessage(), e);
                    }
                    this.currentOffset = offset + 1;
                } else {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                    }
                }
            }
        }

        @Override
        public void close() {
            this.running = false;
            executor.shutdown();
            try {
                executor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // Ignore
            }
        }

    }
}
