package org.apache.aries.events.memory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.apache.aries.events.api.Message;
import org.apache.aries.events.api.Messaging;
import org.apache.aries.events.api.Position;
import org.apache.aries.events.api.Received;
import org.apache.aries.events.api.Seek;
import org.apache.aries.events.api.Subscription;
import org.apache.aries.events.api.Type;
import org.osgi.service.component.annotations.Component;

@Component
@Type("memory")
public class InMemoryMessaging implements Messaging {
    private Map<String, Topic> topics = new ConcurrentHashMap<>();

    @Override
    public Position send(String topicName, Message message) {
        Topic topic = getOrCreate(topicName);
        return topic.send(message);
    }

    @Override
    public Subscription subscribe(String topicName, Position position, Seek seek, Consumer<Received> callback) {
        Topic topic = getOrCreate(topicName);
        return topic.subscribe(position, seek, callback);
    }

    @Override
    public Message newMessage(byte[] payload, Map<String, String> props) {
        return new MemoryMessage(payload, props);
    }

    @Override
    public Position positionFromString(String position) {
        long offset = new Long(position).longValue();
        return new MemoryPosition(offset);
    }

    private Topic getOrCreate(String topicName) {
        return topics.computeIfAbsent(topicName, topicName2 -> new Topic(topicName2));
    }

}
