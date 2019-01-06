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

import org.slf4j.Logger;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * A factory that keeps previously created instances in a cache so that
 * they will get reused if requested repeatedly.
 * Currently there is no cache size limit implemented so this implementation
 * is only good for the use case with limited parameter space.
 * @param <K> key type. Serves as cache key as well as an input parameter for the
 *           factory method. Must provide sensible implementations for
 *          equals and hashCode methods
 * @param <V> result type.
 */
public final class CachingFactory<K, V extends AutoCloseable> implements Closeable {
    

    public static <K2, V2 extends AutoCloseable> CachingFactory<K2, V2> cachingFactory(Function<K2, V2> create) {
        return new CachingFactory<K2, V2>(create);
    }

    /**
     * Find or created a value for the specified key
     * @param arg key instance
     * @return either an existing (cached) value of newly created one.
     */
    public synchronized V get(K arg) {
        return cache.computeIfAbsent(arg, create);
    }

    /**
     * Clears all cached instances properly disposing them.
     */
    public synchronized void clear() {
        cache.values().stream()
            .forEach(CachingFactory::safeClose);
        cache.clear();
    }

    /**
     * Closing this factory properly disposing all cached instances
     */
    @Override
    public void close() {
        clear();
    }

    //*********************************************
    // Private
    //*********************************************

    private static final Logger LOG = getLogger(CachingFactory.class);
    private final Map<K, V> cache = new HashMap<K, V>();
    private final Function<K, V> create;

    private CachingFactory(Function<K, V> create) {
        this.create = create;
    }

    private static void safeClose(AutoCloseable closable) {
        try {
            closable.close();
        } catch (Exception e) {
            LOG.warn(e.getMessage(), e);
        }
    }


}
