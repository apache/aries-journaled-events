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
package org.apache.aries.events.kafka.setup;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.Set;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.aries.events.kafka.setup.KafkaLocal.getKafkaProperties;
import static org.apache.kafka.clients.admin.AdminClient.create;
import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;

public class KafkaBaseTest {

    private static KafkaLocal kafkaLocal;

    private static ZooKeeperLocal zooKeeperLocal;

    private static Logger LOG = LoggerFactory.getLogger(KafkaBaseTest.class);

    @BeforeClass
    public static void startKafka() throws IOException {

        int zkPort = randomAvailablePort();
        String zkDir = createTempDirectory("zk").toString();
        String zkConnect = format("127.0.0.1:%s", zkPort);
        zooKeeperLocal = new ZooKeeperLocal(ZooKeeperLocal.getZooKeeperProperties(zkDir, zkPort));
        LOG.info(format("Started local ZooKeeper server on port %s and dataDirectory %s", zkPort, zkDir));

        int kafkaPort = randomAvailablePort();
        String kafkaLogDir = createTempDirectory("kafka").toString();
        kafkaLocal = new KafkaLocal(getKafkaProperties(kafkaLogDir, kafkaPort, zkConnect));
        LOG.info(format("Started local Kafka on port %s and logDirectory %s", zkConnect, kafkaLogDir));
    }

    @AfterClass
    public static void shutdownKafka() {
        if (kafkaLocal != null) {
            kafkaLocal.stop();
        }
        if (zooKeeperLocal != null) {
            zooKeeperLocal.stop();
        }
    }

    public static KafkaLocal getKafkaLocal() {
        return kafkaLocal;
    }


    public Set<String> listTopics() {
        try (AdminClient admin = buildAdminClient()) {
            ListTopicsResult result = admin.listTopics();
            return result.names().get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to list topics", e);
        }
    }

    public void createTopic(String topicName, int numPartitions) {
        NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) 1);
        try (AdminClient admin = buildAdminClient()) {
            CreateTopicsResult result = admin.createTopics(singletonList(newTopic));
            result.values().get(topicName).get();
            LOG.info(format("created topic %s", topicName));
        } catch (Exception e) {
            throw new RuntimeException(format("Failed to create topic %s", topicName), e);
        }
    }

    public void deleteTopic(String topicName) {
        try (AdminClient admin = buildAdminClient()) {
            DeleteTopicsResult result = admin.deleteTopics(Collections.singleton(topicName));
            result.all().get();
            LOG.info(format("deleted topic %s", topicName));
        } catch (Exception e) {
            throw new RuntimeException(format("Failed to delete topic %s", topicName), e);
        }
    }

    private static int randomAvailablePort() {
        try (ServerSocket ss = new ServerSocket(0)) {
            return ss.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private AdminClient buildAdminClient() {
        return create(singletonMap(BOOTSTRAP_SERVERS_CONFIG, kafkaLocal.getKafkaBootstrapServer()));
    }

}
