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
import java.lang.reflect.Method;
import java.util.Properties;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

public class ZooKeeperLocal {

    private final ZooKeeperServerMain server;

    public ZooKeeperLocal(Properties zkProperties) {
        QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
        try {
            quorumPeerConfig.parseProperties(zkProperties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        ServerConfig serverConfig = new ServerConfig();
        serverConfig.readFrom(quorumPeerConfig);

        server = new ZooKeeperServerMain();

        Thread dt = new Thread(runnable(serverConfig));
        dt.setDaemon(true);
        dt.start();
    }

    public void stop() {
        try {
            Method shutdown = server.getClass().getDeclaredMethod("shutdown");
            shutdown.setAccessible(true);
            shutdown.invoke(server);
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    public static Properties getZooKeeperProperties(String dataDirectory, int port) {
        Properties props = new Properties();
        props.put("dataDir", dataDirectory);
        props.put("clientPort", port);
        return props;
    }

    private Runnable runnable(ServerConfig serverConfig) {
        return () -> {
            try {
                server.runFromConfig(serverConfig);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
