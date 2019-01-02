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

import java.util.Map;

import org.apache.aries.events.api.Message;
import org.apache.aries.events.api.Position;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class KafkaMessage implements Message {

    private final byte[] payload;

    private final Map<String, String> props;

    private final Position position;

    public KafkaMessage(byte[] payload, Map<String, String> props) {
        this(payload, props, null);
    }

    public KafkaMessage(byte[] payload, Map<String, String> props, Position position) {
        this.payload = requireNonNull(payload).clone();
        this.props = unmodifiableMap(requireNonNull(props));
        this.position = position;
    }

    @Override
    public byte[] getPayload() {
        return payload;
    }

    @Override
    public Position getPosition() {
        return position;
    }

    @Override
    public Map<String, String> getProperties() {
        return props;
    }
}
