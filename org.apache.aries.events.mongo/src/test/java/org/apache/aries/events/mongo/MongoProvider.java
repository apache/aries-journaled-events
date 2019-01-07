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

import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.rules.ExternalResource;

import java.util.Optional;
import java.util.logging.Logger;

import static org.junit.Assume.assumeTrue;

/**
 * Provides connection to an external mongodb instance
 * New database gets created for each test and dropped
 * afterwards.
 * Database URL must be provided by mongoUri system
 * property
 */
public class MongoProvider extends ExternalResource {

    MongoCollection<Document> getCollection(String name) {
        return database.getCollection(name);
    }

    //*********************************************
    // Internals
    //*********************************************

    private static final String MONGO_URI_PROP = "aries.events.test.mongoUri";
    private static final String DEFAULT_DB_NAME = "tmp_aries_events_test";
    private MongoDatabase database;
    private MongoClient client;

    @Override
    protected void before() {
	String mongoUri = mongoUri();
	client = MongoClients.create(mongoUri);
	String dbName = Optional.ofNullable(new MongoClientURI(mongoUri).getDatabase())
		.orElse(DEFAULT_DB_NAME);
	database = client.getDatabase(dbName);
    }

    @Override
    protected void after() {
        if (database != null) {
	    database.drop();
	}
        if (client != null) {
	    client.close();
	}
    }

    private static String mongoUri() {
	String result = System.getProperty(MONGO_URI_PROP);
	if (result == null) {
	    String message = "No mongo URI provided.\n" +
		    "  In order to enable mongo tests, define " + MONGO_URI_PROP + " system property\n" +
		    "  to point to a running instance of mongodb.\n" +
		    "  Example:\n" +
		    "        mvn test -D" + MONGO_URI_PROP + "=mongodb://localhost:27017/";
	    System.out.println("WARNING: " + message);
	    assumeTrue(message, false);
	}
	return result;
    }

}
