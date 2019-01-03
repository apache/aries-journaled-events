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

import java.util.Random;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KafkaPositionTest {

    private static final Random RAND = new Random();

    @Test
    public void testGetPartition() throws Exception {
        assertEquals(10, new KafkaPosition(10, 1000).getPartition());
    }

    @Test
    public void testGetOffset() throws Exception {
        assertEquals(1000, new KafkaPosition(10, 1000).getOffset());
    }

    @Test
    public void testPositionToString() throws Exception {
        assertEquals("10:1000", new KafkaPosition(10, 1000).positionToString());
    }

    @Test
    public void testCompareTo() throws Exception {
        assertEquals(0, comparePositions(position(RAND.nextInt(), 5), position(RAND.nextInt(), 5)));
        assertEquals(1, comparePositions(position(RAND.nextInt(), 10), position(RAND.nextInt(), 5)));
        assertEquals(-1, comparePositions(position(RAND.nextInt(), 2), position(RAND.nextInt(), 5)));
    }

    private int comparePositions(KafkaPosition position1, KafkaPosition position2) {
        return position1.compareTo(position2);
    }

    private KafkaPosition position(int partition, long offset) {
        return new KafkaPosition(partition, offset);
    }
}