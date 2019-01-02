package org.apache.aries.events.api;

public final class Received {
    private Position position;
    private Message message;
    
    public Received(Position position, Message message) {
        this.position = position;
        this.message = message;
    }
    
    public Position getPosition() {
        return position;
    }
    
    public Message getMessage() {
        return message;
    }
}
