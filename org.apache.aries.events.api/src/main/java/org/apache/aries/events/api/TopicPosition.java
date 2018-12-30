package org.apache.aries.events.api;

import java.util.Set;

/**
 * Implementation dependent position in a the topic.
 * E.g. For a kafka implementation this would be a list of (partition, offset)
 */
public interface TopicPosition {
    Set<Position> getPositions(); 
}
