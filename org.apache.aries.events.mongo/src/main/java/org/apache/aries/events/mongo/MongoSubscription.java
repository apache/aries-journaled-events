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

import org.apache.aries.events.api.Message;
import org.apache.aries.events.api.Received;
import org.apache.aries.events.api.Seek;
import org.apache.aries.events.api.Subscription;
import org.slf4j.Logger;

import java.util.function.Consumer;

import static java.lang.Thread.currentThread;
import static java.lang.Thread.interrupted;
import static org.apache.aries.events.mongo.MongoPosition.position;
import static org.slf4j.LoggerFactory.getLogger;

final class MongoSubscription implements Subscription {

    //*********************************************
    // Creation
    //*********************************************

    static MongoSubscription subscription(
            MessageReceiver receiver, long index, Seek fallBack, Consumer<Received> consumer
    ) {
        assert index >= 0L : "Illegal log index: [" + index + "]";
        return new MongoSubscription(receiver, index, consumer);
    }

    static MongoSubscription subscription(
            MessageReceiver receiver, Seek seek, Consumer<Received> consumer
    ) {
        switch (seek) {
            case latest:
                return new MongoSubscription(receiver, LATEST_INDEX, consumer);
            case earliest:
                return new MongoSubscription(receiver, EARLIEST_INDEX, consumer);
            default:
                throw new AssertionError(seek);
        }
    }

    //*********************************************
    // Package interface
    //*********************************************

    long index() {
        return index;
    }

    //*********************************************
    // Specialization
    //*********************************************

    @Override
    public void close() {
        receiver.close();
    }

    @Override
    public String toString() {
        return "Subscription" + receiver + '[' + index + ']';
    }

    //*********************************************
    // Private
    //*********************************************

    private static final long LATEST_INDEX = -1;
    private static final long EARLIEST_INDEX = -2;
    private static final Logger LOGGER = getLogger(MongoSubscription.class);
    private final MessageReceiver receiver;
    private long index;
    private final Consumer<Received> consumer;

    private MongoSubscription(
            MessageReceiver receiver, long index, Consumer<Received> consumer
    ) {
        this.consumer = consumer;
        this.receiver = receiver;
        if (index == EARLIEST_INDEX) {
            this.index = receiver.earliestIndex();
        } else if (index == LATEST_INDEX) {
            this.index = receiver.latestIndex();
        } else {
            this.index = index;
        }
        this.index = index == LATEST_INDEX ? receiver.latestIndex() : index;
        startBackgroundThread(() -> poll(receiver), "MongoMessageConsumer-" + receiver);
    }

    private void poll(MessageReceiver receiver) {
        while (!interrupted()) {
            try {
                Message message = receiver.receive(index);
                LOGGER.debug("Received: " + message);
                Received received = new Received(position(index), message);
                consumer.accept(received);
                index += 1L;
            } catch (InterruptedException e) {
                currentThread().interrupt();
            } catch (Exception e) {
                LOGGER.error("Error handling message", e);
            }
        }
        LOGGER.debug("Quitting " + this);
        receiver.close();
    }

    private static Thread startBackgroundThread(Runnable runnable, String threadName) {
        Thread thread = new Thread(runnable, threadName);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

}