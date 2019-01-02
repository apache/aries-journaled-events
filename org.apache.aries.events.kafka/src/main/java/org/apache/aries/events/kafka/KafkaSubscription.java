/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aries.events.kafka;

import java.util.function.Consumer;

import org.apache.aries.events.api.Position;
import org.apache.aries.events.api.Received;
import org.apache.aries.events.api.Subscription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Long.MAX_VALUE;
import static java.time.Duration.ofSeconds;
import static java.util.Objects.requireNonNull;
import static org.apache.aries.events.kafka.KafkaMessaging.toMessage;

public class KafkaSubscription implements Subscription, Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSubscription.class);

    private volatile boolean running = true;

    private final KafkaConsumer<String, byte[]> consumer;

    private final Consumer<Received> callback;

    public KafkaSubscription(KafkaConsumer<String, byte[]> consumer, Consumer<Received> callback) {
        this.consumer = requireNonNull(consumer);
        this.callback = requireNonNull(callback);
    }

    @Override
    public void run() {
        try {
            for (;running;) {
                ConsumerRecords<String, byte[]> records = consumer.poll(ofSeconds(MAX_VALUE));
                records.forEach(record -> callback.accept(toReceived(record)));
            }
        } catch (WakeupException e) {
            if (running) {
                LOG.error("WakeupException while running {}", e.getMessage(), e);
                throw e;
            } else {
                LOG.debug("WakeupException while stopping {}", e.getMessage(), e);
            }
        } catch(Throwable t) {
            LOG.error(String.format("Catch Throwable %s closing subscription", t.getMessage()), t);
            throw t;
        } finally {
            // Close the network connections and sockets
            consumer.close();
        }
    }

    @Override
    public void close() {
        running = false;
        consumer.wakeup();
    }

    private Received toReceived(ConsumerRecord<String, byte[]> record) {
        Position position = new KafkaPosition(record.partition(), record.offset());
        return new Received(position, toMessage(record));
    }

}
