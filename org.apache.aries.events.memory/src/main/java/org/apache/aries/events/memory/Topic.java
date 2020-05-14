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
import java.util.function.Consumer;

import org.apache.aries.events.api.Message;
import org.apache.aries.events.api.Received;
import org.apache.aries.events.api.Seek;
import org.apache.aries.events.api.SubscribeRequestBuilder.SubscribeRequest;
import org.apache.aries.events.api.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Topic {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final String topicName;
    private final Journal<Message> journal;

    public Topic(String topicName, int keepAtLeast) {
        this.topicName = topicName;
        this.journal = new Journal<>(keepAtLeast);
    }

    public synchronized void send(Message message) {
        this.journal.append(message);
        notifyAll();
    }

    public Subscription subscribe(SubscribeRequest request) {
        long startOffset = getStartOffset((MemoryTopicPosition) request.getPosition(), request.getSeek());
        log.debug("Consuming from " + startOffset);
        return new TopicSubscription(startOffset, request.getCallback());
    }

    private long getStartOffset(MemoryTopicPosition position, Seek seek) {
        if (position != null) {
            return position.getOffset();
        } else {
            if (seek == Seek.earliest) {
                return this.journal.getFirstOffset();
            } else {
                return this.journal.getLastOffset() + 1;
            }
        }
    }

    private synchronized Entry<Long, Message> waitNext(long currentOffset) throws InterruptedException {
        Entry<Long, Message> entry = journal.getNext(currentOffset);
        if (entry != null) {
            return entry;
        }
        log.debug("Waiting for next message");
        wait();
        return journal.getNext(currentOffset);
    }

    class TopicSubscription implements Subscription {
        private Consumer<Received> callback;
        private ExecutorService executor;
        private long currentOffset;

        TopicSubscription(long startOffset, Consumer<Received> callback) {
            this.currentOffset = startOffset;
            this.callback = callback;
            String name = "Poller for " + topicName;
            this.executor = Executors.newSingleThreadExecutor(r -> new Thread(r, name));
            this.executor.execute(this::poll);
        }

        private void poll() {
            try {
                while (true) {
                    Entry<Long, Message> entry = waitNext(currentOffset);
                    if (entry != null) {
                        handleMessage(entry);
                    }
                }
            } catch (InterruptedException e) {
                log.debug("Poller thread for consumer on topic " + topicName + " stopped.");
            }
        }

        private void handleMessage(Entry<Long, Message> entry) {
            long offset = entry.getKey();
            try {
                Message message = entry.getValue();
                MemoryPosition position = new MemoryPosition(this.currentOffset);
                Received received = new Received(position, message);
                callback.accept(received);
            } catch (Exception e) {
                log.warn(e.getMessage(), e);
            }
            this.currentOffset = offset + 1;
        }

        @Override
        public void close() {
            executor.shutdown();
            executor.shutdownNow();
        }

    }
}
