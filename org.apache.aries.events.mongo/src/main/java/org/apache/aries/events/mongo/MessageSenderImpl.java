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

import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.IndexOptions;
import org.apache.aries.events.api.Message;
import org.bson.Document;
import org.slf4j.Logger;

import java.util.function.Consumer;

import static com.mongodb.client.model.Filters.lt;
import static java.lang.System.currentTimeMillis;
import static org.apache.aries.events.mongo.Common.Fields.INDEX;
import static org.apache.aries.events.mongo.Common.Fields.PAYLOAD;
import static org.apache.aries.events.mongo.Common.Fields.PROPS;
import static org.apache.aries.events.mongo.Common.Fields.TIME_STAMP;
import static org.apache.aries.events.mongo.Common.upcomingIndex;
import static org.slf4j.LoggerFactory.getLogger;

final class MessageSenderImpl implements MessageSender {

    //*********************************************
    // Creation
    //*********************************************

    static MessageSender messageSender(MongoCollection<Document> col, long maxAge) {
        return new MessageSenderImpl(col, maxAge);
    }

    //*********************************************
    // Specialization
    //*********************************************

    @Override
    public void send(Message message) {
        publish1(message, 3);
        evict();
    }

    @Override
    public void close() {}

    //*********************************************
    // Internals
    //*********************************************

    private static final Logger LOGGER = getLogger(MessageSenderImpl.class);
    private final MongoCollection<Document> collection;
    private long nextEvictionTime = 0L;
    private final long maxAge;

    private MessageSenderImpl(MongoCollection<Document> collection, long maxAge) {
        LOGGER.info("Creating new publisher: " + collection.getNamespace().getCollectionName());
        ensureIndexes(collection);
        this.collection = collection;
        this.maxAge = maxAge;
    }

    private void evict() {
        long currentTime = currentTimeMillis();
        if (currentTime > nextEvictionTime) {
            doEvict(currentTime - maxAge);
            nextEvictionTime = oldestTimeStamp() + maxAge;
        }
    }

    /**
     * Deletes documents that are older then specified threshold but preserving at least one document.
     * At least one document is needed in the collection in order to keep track of the oldest time stamp.
     * @param threshold time threshold (ms). Documents older then specified by threshold are removed.
     */
    private void doEvict(long threshold) {
        collection.find()
                  .projection(new Document(TIME_STAMP, 1))
                  .sort(new Document(TIME_STAMP, -1))
                  .limit(1)
                  .forEach((Consumer<Document>) doc -> {
               long newestTimeStamp = timeStamp(doc);
               long adjustedThreshold = Math.min(threshold, newestTimeStamp);
               collection.deleteMany(lt(TIME_STAMP, adjustedThreshold));
           });
    }

    private void publish1(Message message, int retry) {
        try {
            long index = upcomingIndex(collection);
            collection.insertOne(createDoc(index, message));
        } catch (MongoWriteException e) {
            if (retry > 0) {
                publish1(message, retry - 1);
            } else {
                throw e;
            }
        }
    }

    private long oldestTimeStamp() {
        try (MongoCursor<Document> docs = collection.find().sort(new Document(TIME_STAMP, 1)).iterator()) {
            return docs.hasNext() ? docs.next().get(TIME_STAMP, Long.class) : 0L;
        }
    }

    private static long timeStamp(Document doc) {
        return doc.get(TIME_STAMP, Long.class);
    }

    private Document createDoc(long index, Message message) {
        Document result = new Document();
        result.put(INDEX,      index);
        result.put(TIME_STAMP, currentTimeMillis());
        result.put(PAYLOAD,    message.getPayload());
        result.put(PROPS,      message.getProperties());
        return result;
    }

    private void ensureIndexes(MongoCollection<Document> col) {
        col.createIndex(new Document(INDEX, 1), new IndexOptions().unique(true));
    }

}
