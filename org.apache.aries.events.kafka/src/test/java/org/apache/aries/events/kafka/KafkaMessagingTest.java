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

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.aries.events.api.Message;
import org.apache.aries.events.api.Messaging;
import org.apache.aries.events.api.SubscribeRequestBuilder;
import org.apache.aries.events.api.Subscription;
import org.apache.aries.events.kafka.setup.KafkaBaseTest;
import org.junit.Test;
import org.mockito.Mockito;

import static java.nio.charset.Charset.forName;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class KafkaMessagingTest extends KafkaBaseTest {

    @Test
    public void testPositionFromString() throws Exception {
        Messaging messaging = new KafkaMessaging();
        KafkaPosition kafkaPosition = (KafkaPosition) messaging.positionFromString("0:100");
        assertEquals(0, kafkaPosition.getPartition());
        assertEquals(100, kafkaPosition.getOffset());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPositionFromStringIllegalArgument() throws Exception {
        Messaging messaging = new KafkaMessaging();
        messaging.positionFromString("0:100:23");
    }

    @Test(timeout = 10000)
    public void testSendAndReceive() throws Exception {

        String topic = "test_send_and_receive";
        createTopic(topic, 1);

        KafkaEndpoint kafkaEndpoint = Mockito.mock(KafkaEndpoint.class);
        when(kafkaEndpoint.kafkaBootstrapServers())
                .thenReturn(getKafkaLocal().getKafkaBootstrapServer());
        KafkaMessaging messaging = new KafkaMessaging();
        messaging.activate(kafkaEndpoint);

        byte[] payload = "test".getBytes(forName("UTF-8"));

        Message message = new Message(payload, singletonMap("prop1", "value1"));
        messaging.send(topic, message);

        Semaphore invoked = new Semaphore(0);

        SubscribeRequestBuilder requestBuilder = SubscribeRequestBuilder
                .to(topic, (received) -> invoked.release())
                .startAt(new KafkaPosition(0, 0));

        try (Subscription subscription = messaging.subscribe(requestBuilder)) {
            invoked.tryAcquire(10, TimeUnit.SECONDS);
        }

        messaging.deactivate();
    }

}