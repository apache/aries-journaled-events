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
 * Position of a message in a topic.
 *
 * The positions are ordered. The relative order between
 * two positions can be computed by invoking {@code Comparable#compareTo}.
 * Comparing this position with a specified position will return a negative
 * integer, zero, or a positive integer as this position happened before,
 * happened concurrently, or happened after the specified position.
 * 
 * In systems that use sharding (like Apache Kafka) positions are only comparable 
 * if they have the same key. 
 */
public interface Position extends Comparable<Position> {
    public static final String defaultKey = "default";

    String getKey();

}
