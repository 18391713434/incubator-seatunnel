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

package org.apache.seatunnel.engine.server.master;

import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageConfig;
import org.apache.seatunnel.engine.core.job.ConnectorJar;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.job.SeaTunnelHazelcastClient;
import org.apache.seatunnel.engine.server.task.operation.SendConnectorJarToMemberNodeOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import org.apache.commons.lang3.tuple.ImmutablePair;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.spi.ClientClusterService;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.Collection;
import java.util.List;

@Slf4j
public class ConnectorPackageService {

    private static final ILogger LOGGER = Logger.getLogger(ConnectorPackageService.class);

    private final SeaTunnelServer seaTunnelServer;

    private final ConnectorJarStorageStrategy connectorJarStorageStrategy;

    private final SeaTunnelConfig seaTunnelConfig;

    private final ConnectorJarStorageConfig connectorJarStorageConfig;

    private final NodeEngineImpl nodeEngine;

    public ConnectorPackageService(
            SeaTunnelServer seaTunnelServer, ConnectorPackageHAStorage connectorPackageHAStorage) {
        this.seaTunnelServer = seaTunnelServer;
        this.seaTunnelConfig = seaTunnelServer.getSeaTunnelConfig();
        this.connectorJarStorageConfig =
                seaTunnelConfig.getEngineConfig().getConnectorJarStorageConfig();
        this.nodeEngine = seaTunnelServer.getNodeEngine();
        this.connectorJarStorageStrategy =
                StorageStrategyFactory.of(
                        connectorJarStorageConfig.getStorageMode(),
                        connectorJarStorageConfig,
                        seaTunnelServer,
                        connectorPackageHAStorage);
    }

    public ConnectorJarIdentifier storageConnectorJarFile(long jobId, Data connectorJarData) {
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(seaTunnelConfig.getHazelcastConfig().getClusterName());
        SeaTunnelHazelcastClient seaTunnelHazelcastClient =
                new SeaTunnelHazelcastClient(clientConfig);
        // deserialize connector jar package data
        ConnectorJar connectorJar = nodeEngine.getSerializationService().toObject(connectorJarData);
        ConnectorJarIdentifier connectorJarIdentifier =
                connectorJarStorageStrategy.storageConnectorJarFile(jobId, connectorJar);
        ClientClusterService clientClusterService =
                seaTunnelHazelcastClient.getHazelcastClient().getClientClusterService();
        Address masterNodeAddress = clientClusterService.getMasterMember().getAddress();
        Collection<Member> memberList = clientClusterService.getMemberList();
        memberList.forEach(
                member -> {
                    Address address = member.getAddress();
                    if (!address.equals(masterNodeAddress)) {
                        InvocationFuture<Object> invocationFuture =
                                NodeEngineUtil.sendOperationToMemberNode(
                                        nodeEngine,
                                        new SendConnectorJarToMemberNodeOperation(
                                                seaTunnelHazelcastClient
                                                        .getSerializationService()
                                                        .toData(connectorJar),
                                                seaTunnelHazelcastClient
                                                        .getSerializationService()
                                                        .toData(connectorJarIdentifier)),
                                        address);
                        invocationFuture.join();
                    }
                });
        return connectorJarIdentifier;
    }

    public ImmutablePair<byte[], ConnectorJarIdentifier> readConnectorJarFromLocal(
            ConnectorJarIdentifier connectorJarIdentifier) {
        byte[] bytes =
                connectorJarStorageStrategy.readConnectorJarByteData(
                        new File(connectorJarIdentifier.getStoragePath()));
        return new ImmutablePair<>(bytes, connectorJarIdentifier);
    }

    public void cleanUpWhenJobFinished(
            long jobId, List<ConnectorJarIdentifier> connectorJarIdentifierList) {
        connectorJarStorageStrategy.cleanUpWhenJobFinished(jobId, connectorJarIdentifierList);
    }
}
