package org.apache.aries.events.api;

/**
 * Position in a the topic.
 * E.g. For a kafka implementation this would be a list of (partition, offset) as we do not support partitions 
 * this could simply be like an offset.
 * TODO How do we provide ordering without being too specific?
 */
public interface Position {
    
}
