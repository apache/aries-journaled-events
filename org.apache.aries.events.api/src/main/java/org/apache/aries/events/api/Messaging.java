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

import java.util.Map;

/**
 * Journaled messaging API
 */
public interface Messaging {
    /**
     * Send a message to a topic. When this method returns the message 
     * is safely persisted.
     */
    void send(String topic, Message message);

    /**
     * Subscribe to a topic.
     * The returned subscription must be closed by the caller to unsubscribe.
     *
     * @param request to subscribe
     */
    Subscription subscribe(SubscribeRequest request);

    /**
     * Create a message with payload and metadata
     * @param payload
     * @param props
     * @return
     */
    Message newMessage(byte[] payload, Map<String, String> props);

    /**
     * Deserialize the position from the string
     * 
     * @param position
     * @return
     */
    Position positionFromString(String position);

}
