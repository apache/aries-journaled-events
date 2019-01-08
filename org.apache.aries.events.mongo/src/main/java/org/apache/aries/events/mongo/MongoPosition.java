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

import org.apache.aries.events.api.Position;
import org.apache.aries.events.api.TopicPosition;

class MongoPosition implements Position, TopicPosition {

    static MongoPosition position(long index) {
        return new MongoPosition(index);
    }

    static long index(TopicPosition position) {
        return ((MongoPosition) position).index;
    }
    
    @Override
    public String topicPositionToString() {
        return String.valueOf(index);
    }

    @Override
    public String getKey() {
        return Position.defaultKey;
    }

    @Override
    public int compareTo(Position o) {
        long thatIndex = ((MongoPosition) o).index;
        if (this.index > thatIndex) return 1;
        if (this.index == thatIndex) return 0;
	return -1;
    }

    // *******************************************************
    // Private
    // *******************************************************

    private final long index;

    private MongoPosition(long index) {
	this.index = index;
    }

}
