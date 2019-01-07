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

import java.util.HashMap;
import java.util.Map;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

import static java.lang.String.format;

public class KafkaLocal {

    private final KafkaServerStartable server;

    private final String kafkaBootstrapServer;

    public KafkaLocal(Map<String, Object> kafkaProperties) {
        KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);
        kafkaBootstrapServer = format("%s:%s", kafkaConfig.hostName(), kafkaConfig.port());
        server = new KafkaServerStartable(kafkaConfig);
        server.startup();
    }

    public void stop() {
        server.shutdown();
    }

    public String getKafkaBootstrapServer() {
        return kafkaBootstrapServer;
    }

    public static Map<String, Object> getKafkaProperties(String logDir, int port, String zkConnect) {
        Map<String, Object> props = new HashMap<>();
        props.put("host.name", "localhost");
        props.put("log.dir", logDir);
        props.put("port", port);
        props.put("zookeeper.connect", zkConnect);
        return props;
    }
}
