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

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.aries.events.api.Message;
import org.apache.aries.events.api.Messaging;
import org.apache.aries.events.api.Position;
import org.apache.aries.events.api.SubscribeRequestBuilder;
import org.apache.aries.events.api.SubscribeRequestBuilder.SubscribeRequest;
import org.apache.aries.events.api.Subscription;
import org.apache.aries.events.api.TopicPosition;
import org.bson.Document;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.metatype.annotations.Designate;

import java.util.Optional;

import static org.apache.aries.events.mongo.Common.DEFAULT_DB_NAME;
import static org.apache.aries.events.mongo.MongoPosition.index;
import static org.apache.aries.events.mongo.MongoPosition.position;
import static org.apache.aries.events.mongo.MongoSubscription.subscription;
import static org.apache.aries.events.mongo.MessageSenderImpl.messageSender;
import static org.apache.aries.events.mongo.MessageReceiverImpl.messageReceiver;
import static org.apache.aries.events.mongo.CachingFactory.cachingFactory;
import static org.osgi.service.component.annotations.ConfigurationPolicy.REQUIRE;

@Component(service = Messaging.class, configurationPolicy = REQUIRE)
@Designate(ocd = MongoEndpoint.class)
public class MongoMessaging implements Messaging {

    @Override
    public void send(String topic, Message message) {
        MessageSender sender = senderFactory.get(topic);
        sender.send(message);
    }

    @Override
    public Subscription subscribe(SubscribeRequestBuilder requestBuilder) {
        SubscribeRequest request = requestBuilder.build();
        MongoCollection<Document> collection = database.getCollection(request.getTopic());
        MessageReceiver receiver = messageReceiver(collection);
        return subscription(receiver, index(request.getPosition()), request.getSeek(), request.getCallback());
    }

    @Override
    public TopicPosition positionFromString(String position) {
        long index = Long.parseLong(position);
        return position(index);
    }

    // *******************************************************
    // Private
    // *******************************************************

    private CachingFactory<String, MessageSender> senderFactory;
    private MongoClient client;
    private MongoDatabase database;

    @Activate
    protected void activate(MongoEndpoint config) {
        MongoClientURI uri = new MongoClientURI(config.mongoUri());
        client = new MongoClient(uri);
        String dbName = Optional.ofNullable(uri.getDatabase()).orElse(DEFAULT_DB_NAME);
        this.database = client.getDatabase(dbName);
        this.senderFactory = cachingFactory(topic -> {
            MongoCollection<Document> collection = database.getCollection(topic);
            return messageSender(collection, config.maxAge());
        });
    }

    @Deactivate
    protected void deactivate() {
        client.close();
    }

}
