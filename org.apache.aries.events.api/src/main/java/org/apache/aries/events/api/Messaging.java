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

/**
 * Journaled messaging API
 */
public interface Messaging {
    /**
     * Send a message to a topic. When this method returns the message 
     * is safely persisted.
     *
     * Messages can be consumed by subscribing to the topic via the #subscribe method.
     *
     * Two messages sent sequentially to the same topic by the same
     * thread, are guaranteed to be consumed in the same order by all subscribers.
     */
    void send(String topic, Message message);

    /**
     * Subscribe to a topic.
     * The returned subscription must be closed by the caller to unsubscribe.
     *
     * @param request to subscribe
     */
    Subscription subscribe(SubscribeRequestBuilder request);

    /**
     * Deserialize the position from the string
     * 
     * @param position
     * @return
     */
    Position positionFromString(String position);

}
