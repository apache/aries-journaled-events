package org.apache.aries.events.api;

import java.io.Closeable;

public interface Subscription extends Closeable {

    @Override
    void close();
}
