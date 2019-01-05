package org.apache.aries.events.memory;

import static org.apache.aries.events.api.SubscribeRequest.to;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doThrow;
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

import org.apache.aries.events.api.Message;
import org.apache.aries.events.api.Messaging;
import org.apache.aries.events.api.Position;
import org.apache.aries.events.api.Received;
import org.apache.aries.events.api.Seek;
import org.apache.aries.events.api.SubscribeRequest;
import org.apache.aries.events.api.Subscription;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;

public class MessagingTest {
    
    @Mock
    private Consumer<Received> callback;
    
    @Captor
    private ArgumentCaptor<Received> messageCaptor;

    private Set<Subscription> subscriptions = new HashSet<>();

    private Messaging messaging;
    
    @Before
    public void before() {
        initMocks(this);
        messaging = new InMemoryMessaging();
    }
    
    @After
    public void after() {
        subscriptions.forEach(Subscription::close);
    }
    
    @Test
    public void testPositionFromString() {
        Position pos = messaging.positionFromString("1");
        assertThat(pos.compareTo(new MemoryPosition(1)), equalTo(0));
        assertThat(pos.positionToString(), equalTo("1"));
    }
    
    @Test
    public void testSend() {
        subscribe(to("test", callback).seek(Seek.earliest));
        String content = "testcontent";
        send("test", content);
        verify(callback, timeout(1000)).accept(messageCaptor.capture());
        Received received = messageCaptor.getValue();
        assertThat(received.getMessage().getPayload(), equalTo(toBytes(content)));
        assertEquals(0, received.getPosition().compareTo(new MemoryPosition(0)));
        assertThat(received.getMessage().getProperties().size(), equalTo(1));
        assertThat(received.getMessage().getProperties().get("my"), equalTo("testvalue"));
    }
    
    @Test(expected=NullPointerException.class)
    public void testInvalidSubscribe() {
        subscribe(to("test", callback).seek(null));
    }
    
    @Test
    public void testExceptionInHandler() {
        doThrow(new RuntimeException("Expected exception")).when(callback).accept(Mockito.any(Received.class));
        subscribe(to("test", callback));
        send("test", "testcontent");
        verify(callback, timeout(1000)).accept(messageCaptor.capture());
    }

    @Test
    public void testEarliestBefore() {
        subscribe(to("test", callback).seek(Seek.earliest));
        send("test", "testcontent");
        send("test", "testcontent2");
        verify(callback, timeout(1000).times(2)).accept(messageCaptor.capture());
        assertThat(messageContents(), contains("testcontent", "testcontent2"));
    }
    
    @Test
    public void testEarliestAfter() {
        send("test", "testcontent");
        subscribe(to("test", callback).seek(Seek.earliest));
        send("test", "testcontent2");
        verify(callback, timeout(1000).times(2)).accept(messageCaptor.capture());
        assertThat(messageContents(), contains("testcontent", "testcontent2"));
    }
    
    @Test
    public void testLatestBefore() {
        subscribe(to("test", callback));
        send("test", "testcontent");
        send("test", "testcontent2");
        verify(callback, timeout(1000).times(2)).accept(messageCaptor.capture());
        assertThat(messageContents(), contains("testcontent", "testcontent2"));
    }
    
    @Test
    public void testLatest() {
        send("test", "testcontent");
        subscribe(to("test", callback));
        send("test", "testcontent2");
        verify(callback, timeout(1000)).accept(messageCaptor.capture());
        assertThat(messageContents(), contains("testcontent2"));
    }
    
    @Test
    public void testFrom1() {
        send("test", "testcontent");
        send("test", "testcontent2");
        subscribe(to("test", callback).startAt(new MemoryPosition(1l)).seek(Seek.earliest));
        verify(callback, timeout(1000)).accept(messageCaptor.capture());
        assertThat(messageContents(), contains("testcontent2"));
    }
    
    private void subscribe(SubscribeRequest request) {
        this.subscriptions.add(messaging.subscribe(request));
    }

    private List<String> messageContents() {
        return messageCaptor.getAllValues().stream()
                .map(this::getContent).collect(Collectors.toList());
    }
    
    private String getContent(Received rec) {
        return new String(rec.getMessage().getPayload(), Charset.forName("UTF-8"));
    }
    
    private void send(String topic, String content) {
        Map<String, String> props = new HashMap<String, String>();
        props.put("my", "testvalue");
        Message message = messaging.newMessage(toBytes(content), props);
        messaging.send(topic, message);
    }

    private byte[] toBytes(String content) {
        return content.getBytes(Charset.forName("UTF-8"));
    }
    
}
