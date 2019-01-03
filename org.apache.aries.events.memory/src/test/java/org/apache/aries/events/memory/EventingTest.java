package org.apache.aries.events.memory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.aries.events.api.Event;
import org.apache.aries.events.api.Eventing;
import org.apache.aries.events.api.Position;
import org.apache.aries.events.api.Received;
import org.apache.aries.events.api.Seek;
import org.apache.aries.events.api.Subscription;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

public class EventingTest {
    
    @Mock
    private Consumer<Received> callback;
    
    @Captor
    private ArgumentCaptor<Received> messageCaptor;

    private Set<Subscription> subscriptions = new HashSet<>();

    private Eventing eventing;
    
    @Before
    public void before() {
        initMocks(this);
        eventing = new InMemoryEventing();
    }
    
    @After
    public void after() {
        subscriptions.forEach(Subscription::close);
    }
    
    @Test
    public void testPositionFromString() {
        Position pos = eventing.positionFromString("1");
        assertThat(pos.getOffset(), equalTo(1l));
    }
    
    @Test
    public void testSend() {
        subscriptions.add(eventing.subscribe("test", null, Seek.earliest, callback));
        String content = "testcontent";
        Position pos = send("test", content);
        assertThat(pos.toString(), equalTo("0"));
        
        verify(callback, timeout(1000)).accept(messageCaptor.capture());
        Received received = messageCaptor.getValue();
        assertThat(received.getEvent().getPayload(), equalTo(toBytes(content)));
        assertThat(received.getPosition().getOffset(), equalTo(0l));
        assertThat(received.getEvent().getProperties().size(), equalTo(1));
        assertThat(received.getEvent().getProperties().get("my"), equalTo("testvalue"));
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testInvalid() {
        eventing.subscribe("test", null, null, callback);
    }

    @Test
    public void testEarliestBefore() {
        subscriptions.add(eventing.subscribe("test", null, Seek.earliest, callback));
        send("test", "testcontent");
        send("test", "testcontent2");
        verify(callback, timeout(1000).times(2)).accept(messageCaptor.capture());
        assertThat(messageContents(), contains("testcontent", "testcontent2"));
    }
    
    @Test
    public void testEarliestAfter() {
        send("test", "testcontent");
        subscriptions.add(eventing.subscribe("test", null, Seek.earliest, callback));
        send("test", "testcontent2");
        verify(callback, timeout(1000).times(2)).accept(messageCaptor.capture());
        assertThat(messageContents(), contains("testcontent", "testcontent2"));
    }
    
    @Test
    public void testLatestBefore() {
        subscriptions.add(eventing.subscribe("test", null, Seek.latest, callback));
        send("test", "testcontent");
        send("test", "testcontent2");
        verify(callback, timeout(1000).times(2)).accept(messageCaptor.capture());
        assertThat(messageContents(), contains("testcontent", "testcontent2"));
    }
    
    @Test
    public void testLatest() {
        send("test", "testcontent");
        subscriptions.add(eventing.subscribe("test", null, Seek.latest, callback));
        send("test", "testcontent2");
        verify(callback, timeout(1000)).accept(messageCaptor.capture());
        assertThat(messageContents(), contains("testcontent2"));
    }
    
    @Test
    public void testFrom1() {
        send("test", "testcontent");
        send("test", "testcontent2");
        subscriptions.add(eventing.subscribe("test", new MemoryPosition(1l), Seek.earliest, callback));
        verify(callback, timeout(1000)).accept(messageCaptor.capture());
        assertThat(messageContents(), contains("testcontent2"));
    }

    private List<String> messageContents() {
        return messageCaptor.getAllValues().stream()
                .map(this::getContent).collect(Collectors.toList());
    }
    
    private String getContent(Received rec) {
        return new String(rec.getEvent().getPayload(), Charset.forName("UTF-8"));
    }
    
    private Position send(String topic, String content) {
        Map<String, String> props = new HashMap<String, String>();
        props.put("my", "testvalue");
        Event event = eventing.newEvent(toBytes(content), props);
        return eventing.send(topic, event);
    }

    private byte[] toBytes(String content) {
        return content.getBytes(Charset.forName("UTF-8"));
    }
    
}
