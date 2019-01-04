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

import org.apache.aries.events.api.Message;

public interface MessageReceiver extends AutoCloseable {

    /** returns data entry for the specified offset.
     * If necessary waits until data is available.
     * If data entry at the specified offset has
     * been evicted, throws NoSuchElement exception
     * @param index an offset to the desired entry
     * @return requested data entry together with the
     *         offset for the next data entry
     */
    Message receive(long index) throws InterruptedException;

    /** returns the index of the earliest available
     * data entry. It also causes the receiver to
     * pre-fetch and cache a batch of earliest available
     * entries thus giving the user a chance to consume
     * them and catch up before they get evicted
     * @return an index of the first available data entry or
     * 0 if the log is empty
     */
    long earliestIndex();

    /** returns the index of the next available data
     *  entry.
     *  The returned index points to the entry yet to
     *  be inserted into the log.
     *  @return index of the data entry that will be
     *  inserted next. 0 if the log is empty.
     */
    long latestIndex();

    @Override
    void close();

}
