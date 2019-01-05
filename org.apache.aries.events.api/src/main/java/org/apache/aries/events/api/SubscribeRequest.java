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

import static java.util.Objects.requireNonNull;

import java.util.function.Consumer;

import javax.annotation.ParametersAreNonnullByDefault;

/**
 * A Subscription request to consume messages from a topic.
 */
@ParametersAreNonnullByDefault
public final class SubscribeRequest {
    private final String topic;
    private final Consumer<Received> callback;
    private Position position;
    private Seek seek = Seek.latest;
    
    private SubscribeRequest(String topic, Consumer<Received> callback) {
        this.topic = topic;
        this.callback = callback;
    }

    /**
     * Build a subscription request for the given topic and {@code Consumer} callback.
     *
     * @param topic to consume from
     * @param callback to be invoked for each message consumed
     * @return a new subscription request
     */
    public static SubscribeRequest to(String topic, Consumer<Received> callback) {
        return new SubscribeRequest(topic, callback);
    }

    /**
     * Set the {@code Position} position to start consuming from.
     *
     * @param position in the topic to start consuming from
     * @return the updated subscribe request
     */
    public SubscribeRequest startAt(Position position) {
        this.position = position;
        return this;
    }

    /**
     * Set the earliest or latest position to start consuming from
     * when the position is {@code null} or not valid. By default,
     * seek is set to {@link Seek#earliest}.
     *
     * @param seek where to start consuming when no valid position is specified
     * @return the updated subscribe request
     */
    public SubscribeRequest seek(Seek seek) {
        this.seek = requireNonNull(seek, "Seek must not be null");
        return this;
    }
    
    public String getTopic() {
        return topic;
    }
    
    public Position getPosition() {
        return position;
    }
    
    public Seek getSeek() {
        return seek;
    }
    
    public Consumer<Received> getCallback() {
        return callback;
    }
}
