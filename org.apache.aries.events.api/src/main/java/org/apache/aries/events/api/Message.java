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
package org.apache.aries.events.api;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

/**
 * TODO If we allow wild card consumption then a message also needs a topic
 * 
 * The property key "key" is a special property. For systems that support sharding
 * the key can be used to indirectly select the partition to be used.
 */
public final class Message {
    public static final String KEY = "key";

    private final byte[] payload;
    private final Map<String, String> properties;

    public Message(byte[] payload, Map<String, String> properties) {
        requireNonNull(payload);
        requireNonNull(properties);
        this.payload = payload.clone();
        this.properties = unmodifiableMap(new HashMap<>(properties));
    }

    public byte[] getPayload() {
        return payload.clone();
    }
    
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "Message" + properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return Arrays.equals(payload, message.payload) &&
                properties.equals(message.properties);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(properties);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }

}
