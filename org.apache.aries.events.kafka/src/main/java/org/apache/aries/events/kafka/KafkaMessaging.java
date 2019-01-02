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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.aries.events.api.Message;
import org.apache.aries.events.api.Messaging;
import org.apache.aries.events.api.Position;
import org.apache.aries.events.api.Received;
import org.apache.aries.events.api.Seek;
import org.apache.aries.events.api.Subscription;
import org.apache.aries.events.api.Type;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.metatype.annotations.Designate;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singleton;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.StreamSupport.stream;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

@Type("kafka")
@Component(service = Messaging.class, configurationPolicy = ConfigurationPolicy.REQUIRE)
@Designate(ocd = KafkaEndpoint.class)
public class KafkaMessaging implements Messaging {

    /**
     * The partition to send and receive records.
     */
    private static final int PARTITION = 0;

    /**
     * Shared Kafka producer instance ({@code KafkaProducer}s are thread-safe).
     */
    private KafkaProducer<String, byte[]> producer;

    private Map<String, Object> producerConfig;

    private KafkaEndpoint endPoint;

    @Activate
    public void activate(KafkaEndpoint endPoint) {
        this.endPoint = endPoint;
        producerConfig = new HashMap<>();
        producerConfig.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerConfig.put(BOOTSTRAP_SERVERS_CONFIG, endPoint.kafkaBootstrapServers());
        // We favour durability over throughput
        // and thus requires full acknowledgment
        // from replica leader and followers.
        producerConfig.put(ACKS_CONFIG, "all");
        producerConfig = unmodifiableMap(producerConfig);
    }

    @Deactivate
    public void deactivate() {
        closeQuietly(producer);
    }

    @Override
    public Position send(String topic, Message message) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(topic, PARTITION, null, message.getPayload(), toHeaders(message.getProperties()));
        try {
            RecordMetadata metadata = kafkaProducer().send(record).get();
            return new KafkaPosition(metadata.partition(), metadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(format("Failed to send mesage on topic %s", topic), e);
        }
    }

    @Override
    public Subscription subscribe(String topic, Position position, Seek seek, Consumer<Received> callback) {
        KafkaConsumer<String, byte[]> consumer = buildKafkaConsumer(seek);
        assignAndSeek(consumer, topic, position);
        Subscription subscription = new KafkaSubscription(consumer, callback);
        // TODO pool the threads
        Thread thread = new Thread();
        thread.setDaemon(true);
        thread.start();
        return subscription;
    }

    @Override
    public Message newMessage(byte[] payload, Map<String, String> props) {
        return new KafkaMessage(payload, props);
    }

    @Override
    public Position positionFromString(String position) {
        String[] chunks = position.split(":");
        if (chunks.length != 2) {
            throw new IllegalArgumentException(format("Illegal position format %s", position));
        }
        return new KafkaPosition(parseInt(chunks[0]), parseLong(chunks[1]));
    }

    static String positionToString(Position position) {
        KafkaPosition kafkaPosition = asKafkaPosition(position);
        return format("%s:%s", kafkaPosition.getPartition(), kafkaPosition.getOffset());
    }

    static Iterable<Header> toHeaders(Map<String, String> properties) {
        return properties.entrySet().stream()
                .map(KafkaMessaging::toHeader)
                .collect(Collectors.toList());
    }

    static Map<String, String> toProperties(Headers headers) {
        return stream(headers.spliterator(), true)
                .collect(Collectors.toMap(Header::key, header -> new String(header.value(), UTF_8)));
    }

    static RecordHeader toHeader(Map.Entry<String, String> property) {
        return new RecordHeader(property.getKey(), property.getValue().getBytes(UTF_8));
    }

    static Message toMessage(ConsumerRecord<String, byte[]> record) {
        return new KafkaMessage(record.value(), toProperties(record.headers()));
    }


    private synchronized KafkaProducer<String, byte[]> kafkaProducer() {
        if (producer == null) {
            producer = new KafkaProducer<>(producerConfig);
        }
        return producer;
    }

    private KafkaConsumer<String, byte[]> buildKafkaConsumer(Seek seek) {

        String groupId = UUID.randomUUID().toString();

        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(BOOTSTRAP_SERVERS_CONFIG, endPoint.kafkaBootstrapServers());
        consumerConfig.put(GROUP_ID_CONFIG, groupId);
        consumerConfig.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerConfig.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerConfig.put(AUTO_OFFSET_RESET_CONFIG, seek.name());

        return new KafkaConsumer<>(unmodifiableMap(consumerConfig));
    }

    private void assignAndSeek(KafkaConsumer consumer, String topicName, Position position){
        KafkaPosition kafkaPosition = asKafkaPosition(position);
        TopicPartition topicPartition = new TopicPartition(topicName, PARTITION);
        consumer.assign(singleton(topicPartition));
        consumer.seek(topicPartition, kafkaPosition.getOffset());
    }

    private void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ignore) {
                // ignore
            }
        }
    }

    private static KafkaPosition asKafkaPosition(Position position) {
        if (! KafkaPosition.class.isInstance(position)) {
            throw new IllegalArgumentException(format("Position %s must be and instance of %s", position, KafkaPosition.class.getCanonicalName()));
        }
        return (KafkaPosition) position;
    }


}
