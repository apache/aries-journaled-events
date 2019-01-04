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
import org.bson.Document;

import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Indexes.descending;
import static org.apache.aries.events.mongo.Common.Fields.INDEX;

/**
 * Common string definitions
 */
@SuppressWarnings({"HardCodedStringLiteral", "InterfaceNeverImplemented"})
interface Common {

    String DEFAULT_DB_NAME = "aem-replication";

    /** MongoDB field names */
    interface Fields {
        String INDEX = "i";
        String TIME_STAMP = "t";
        String PAYLOAD = "d";
        String PROPS = "p";
    }

    /**
     * Returns the next available index in the collection
     * @param col collection to check. The collection must contain
     *            log messages published by a Publisher instance
     * @return the index that should be assigned to the next message when
     * it gets published
     */
    static long upcomingIndex(MongoCollection<Document> col) {
        Document doc = col.find(lte(INDEX, Long.MAX_VALUE))
                          .sort(descending(INDEX))
                          .first();
        if (doc != null) {
            long latestAvailable = doc.getLong(INDEX);
            return latestAvailable + 1L;
        } else {
            return 0L;
        }
    }

}
