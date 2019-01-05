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

import org.apache.aries.events.api.Message;
import org.apache.aries.events.api.Messaging;
import org.apache.aries.events.api.Position;
import org.apache.aries.events.api.SubscribeRequest;
import org.apache.aries.events.api.Subscription;
import org.apache.aries.events.api.Type;
import org.osgi.service.component.annotations.Component;

@Component
@Type("memory")
public class InMemoryMessaging implements Messaging {
    private final Map<String, Topic> topics = new ConcurrentHashMap<>();
    private final int keepAtLeast;
    
    public InMemoryMessaging() {
        this(10000);
    }

    public InMemoryMessaging(int keepAtLeast) {
        this.keepAtLeast = keepAtLeast;
        
    }

    @Override
    public void send(String topicName, Message message) {
        Topic topic = getOrCreate(topicName);
        topic.send(message);
    }

    @Override
    public Subscription subscribe(SubscribeRequest request) {
        Topic topic = getOrCreate(request.getTopic());
        return topic.subscribe(request);
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
        return topics.computeIfAbsent(topicName, topicName2 -> new Topic(topicName2, keepAtLeast));
    }

}
