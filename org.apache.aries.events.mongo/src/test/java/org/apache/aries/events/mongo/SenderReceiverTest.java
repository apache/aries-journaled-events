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

import com.mongodb.client.MongoCollection;
import org.apache.aries.events.api.Message;
import org.bson.Document;
import org.junit.Rule;
import org.junit.Test;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import static java.util.Collections.emptyMap;
import static org.apache.aries.events.mongo.MessageReceiverImpl.messageReceiver;
import static org.apache.aries.events.mongo.MessageSenderImpl.messageSender;
import static org.junit.Assert.assertEquals;

public class SenderReceiverTest {

    @Test public void testReplicate() throws InterruptedException {
        MongoCollection<Document> collection = mongoProvider.getCollection("events");
        MessageSender sender = messageSender(collection, 1000 * 60 * 60 * 24 * 7);
        MessageReceiver receiver = messageReceiver(collection);
        Message expected = new Message(new byte[]{ 1, 2, 3 }, mapOf(
                keyVal("key1", "val1"),
                keyVal("key2", "val2"))
        );
        sender.send(expected);
        sender.send(expected);
        Message actual = receiver.receive(0);
        assertEquals(expected, actual);
    }

    @Test(expected = NoSuchElementException.class)
    public void testEvicted() throws InterruptedException {
        MongoCollection<Document> collection = mongoProvider.getCollection("events");
        MessageSender sender = messageSender(collection, 0);
        MessageReceiver receiver = messageReceiver(collection);
        Message expected = new Message(new byte[] { 1, 2, 3}, emptyMap());
        sender.send(expected);
        sender.send(expected);
        receiver.receive(0);
    }

    //*********************************************
    // Internals
    //*********************************************

    private MongoCollection<Document> collection;

    @Rule
    public MongoProvider mongoProvider = new MongoProvider();

    private static Map.Entry<String, String> keyVal(String key, String value) {
        return new SimpleEntry<>(key, value);
    }

    private static Map<String, String> mapOf(Map.Entry<String, String>... mappings) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : mappings) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

}
