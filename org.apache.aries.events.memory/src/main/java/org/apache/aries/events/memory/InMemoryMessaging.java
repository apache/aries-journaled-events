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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.apache.aries.events.api.Message;
import org.apache.aries.events.api.Messaging;
import org.apache.aries.events.api.Position;
import org.apache.aries.events.api.Received;
import org.apache.aries.events.api.Seek;
import org.apache.aries.events.api.Subscription;
import org.apache.aries.events.api.Type;
import org.osgi.service.component.annotations.Component;

@Component
@Type("memory")
public class InMemoryMessaging implements Messaging {
    private Map<String, Topic> topics = new ConcurrentHashMap<>();

    @Override
    public Position send(String topicName, Message message) {
        Topic topic = getOrCreate(topicName);
        return topic.send(message);
    }

    @Override
    public Subscription subscribe(String topicName, Position position, Seek seek, Consumer<Received> callback) {
        Topic topic = getOrCreate(topicName);
        return topic.subscribe(position, seek, callback);
    }

    @Override
    public Message newMessage(byte[] payload, Map<String, String> props) {
        return new MemoryMessage(payload, props);
    }

    @Override
    public Position positionFromString(String position) {
        long offset = Long.parseLong(position);
        return new MemoryPosition(offset);
    }

    private Topic getOrCreate(String topicName) {
        return topics.computeIfAbsent(topicName, Topic::new);
    }

}
