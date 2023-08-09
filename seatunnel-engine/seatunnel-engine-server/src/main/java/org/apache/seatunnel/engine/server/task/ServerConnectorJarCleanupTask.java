/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server.task;

import com.hazelcast.logging.ILogger;

import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/*
Cleanup task for connector jar package on execution node.
 */
public class ServerConnectorJarCleanupTask extends TimerTask {

    private final ILogger LOGGER;

    private final Consumer<URL> cleanupCallback;

    private final ConcurrentHashMap<URL, ServerConnectorPackageClient.ExpiryTime>
            connectorJarExpiryTimes;

    public ServerConnectorJarCleanupTask(
            ILogger LOGGER,
            Consumer<URL> cleanupCallback,
            ConcurrentHashMap<URL, ServerConnectorPackageClient.ExpiryTime>
                    connectorJarExpiryTimes) {
        this.LOGGER = LOGGER;
        this.cleanupCallback = cleanupCallback;
        this.connectorJarExpiryTimes = connectorJarExpiryTimes;
    }

    @Override
    public void run() {
        synchronized (connectorJarExpiryTimes) {
            Iterator<Map.Entry<URL, ServerConnectorPackageClient.ExpiryTime>> iterator =
                    connectorJarExpiryTimes.entrySet().iterator();
            final long currentTimeMillis = System.currentTimeMillis();
            while (iterator.hasNext()) {
                Map.Entry<URL, ServerConnectorPackageClient.ExpiryTime> entry = iterator.next();
                if (entry.getValue().keepUntil > 0
                        && currentTimeMillis >= entry.getValue().keepUntil) {
                    URL connectorJarUrl = entry.getKey();
                    cleanupCallback.accept(connectorJarUrl);
                    connectorJarExpiryTimes.remove(connectorJarUrl);
                }
            }
        }
    }
}
