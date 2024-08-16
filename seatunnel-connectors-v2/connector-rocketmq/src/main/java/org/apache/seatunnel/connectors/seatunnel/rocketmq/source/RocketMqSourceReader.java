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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.source;

import org.apache.seatunnel.shade.com.google.common.collect.Maps;
import org.apache.seatunnel.shade.com.google.common.collect.Sets;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.exception.RocketMqConnectorException;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

@Slf4j
public class RocketMqSourceReader implements SourceReader<SeaTunnelRow, RocketMqSourceSplit> {

    private static final long THREAD_WAIT_TIME = 500L;

    private final SourceReader.Context context;
    private final ConsumerMetadata metadata;
    private final Set<RocketMqSourceSplit> sourceSplits;
    private final Map<Long, Map<MessageQueue, Long>> checkpointOffsets;
    private final Map<MessageQueue, RocketMqConsumerThread> consumerThreads;
    private final ExecutorService executorService;
    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;

    private final LinkedBlockingQueue<RocketMqSourceSplit> pendingPartitionsQueue;

    private volatile boolean running = false;

    public RocketMqSourceReader(
            ConsumerMetadata metadata,
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            SourceReader.Context context) {
        this.metadata = metadata;
        this.context = context;
        this.sourceSplits = new HashSet<>();
        this.deserializationSchema = deserializationSchema;
        this.consumerThreads = new ConcurrentHashMap<>();
        this.checkpointOffsets = new ConcurrentHashMap<>();
        this.executorService =
                Executors.newCachedThreadPool(r -> new Thread(r, "RocketMq Source Data Consumer"));
        pendingPartitionsQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public void open() throws Exception {
        // No-op
    }

    @Override
    public void close() throws IOException {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (!running) {
            Thread.sleep(THREAD_WAIT_TIME);
            return;
        }
        while (pendingPartitionsQueue.size() != 0) {
            // 将阻塞队列中的分片数据加入到sourceSplits
            sourceSplits.add(pendingPartitionsQueue.poll());
        }
        sourceSplits.forEach(
                sourceSplit ->
                        consumerThreads.computeIfAbsent(
                                sourceSplit.getMessageQueue(),
                                s -> {
                                    // 启动消费者线程进行异步消费
                                    RocketMqConsumerThread thread =
                                            new RocketMqConsumerThread(metadata);
                                    executorService.submit(thread);
                                    return thread;
                                }));
        sourceSplits.forEach(
                sourceSplit -> {
                    CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                    try {
                        // 将分片放入到启动的消费者线程中的阻塞队列中，消费者会从阻塞队列中取数据
                        consumerThreads
                                .get(sourceSplit.getMessageQueue())
                                .getTasks()
                                .put(
                                        consumer -> {
                                            try {
                                                Set<MessageQueue> messageQueues =
                                                        Sets.newHashSet(
                                                                sourceSplit.getMessageQueue());
                                                consumer.assign(messageQueues);
                                                if (sourceSplit.getStartOffset() >= 0) {
                                                    // 重设Partition分区消费偏移量Offset
                                                    consumer.seek(
                                                            sourceSplit.getMessageQueue(),
                                                            sourceSplit.getStartOffset());
                                                }
                                                List<MessageExt> records =
                                                        consumer.poll(
                                                                metadata.getBaseConfig()
                                                                        .getPollTimeoutMillis());
                                                if (records.isEmpty()) {
                                                    log.warn(
                                                            "Rocketmq consumer can not pull data, split {}, start offset {}, end offset {}",
                                                            sourceSplit.getMessageQueue(),
                                                            sourceSplit.getStartOffset(),
                                                            sourceSplit.getEndOffset());
                                                }
                                                // 按照Topic、Broker，Partition对消息进行分组
                                                Map<MessageQueue, List<MessageExt>> groupRecords =
                                                        records.stream()
                                                                .collect(
                                                                        Collectors.groupingBy(
                                                                                record ->
                                                                                        new MessageQueue(
                                                                                                record
                                                                                                        .getTopic(),
                                                                                                record
                                                                                                        .getBrokerName(),
                                                                                                record
                                                                                                        .getQueueId())));
                                                for (MessageQueue messageQueue : messageQueues) {
                                                    // 按照partition的顺序进行消息写入
                                                    if (!groupRecords.containsKey(messageQueue)) {
                                                        continue;
                                                    }
                                                    List<MessageExt> messages =
                                                            groupRecords.get(messageQueue);
                                                    for (MessageExt record : messages) {
                                                        // 将消息写入到output中进行收集
                                                        deserializationSchema.deserialize(
                                                                record.getBody(), output);
                                                        if (Boundedness.BOUNDED.equals(
                                                                        context.getBoundedness())
                                                                && record.getQueueOffset()
                                                                        >= sourceSplit
                                                                                .getEndOffset()) {
                                                            break;
                                                        }
                                                    }
                                                    long lastOffset = -1;
                                                    if (!messages.isEmpty()) {
                                                        lastOffset =
                                                                messages.get(messages.size() - 1)
                                                                        .getQueueOffset();
                                                        sourceSplit.setStartOffset(lastOffset);
                                                    }

                                                    if (lastOffset >= sourceSplit.getEndOffset()) {
                                                        sourceSplit.setEndOffset(lastOffset);
                                                    }
                                                }
                                            } catch (Exception e) {
                                                completableFuture.completeExceptionally(e);
                                            }
                                            completableFuture.complete(null);
                                        });
                    } catch (InterruptedException e) {
                        throw new RocketMqConnectorException(
                                RocketMqConnectorErrorCode.CONSUME_DATA_FAILED, e);
                    }
                    completableFuture.join();
                });

        if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
            // signal to the source that we have reached the end of the data.
            // 如果数据流是有界的 (Boundedness.BOUNDED)，且所有分区的消息都已消费完毕
            // 调用 context.signalNoMoreElement() 通知源已经没有更多数据可消费
            context.signalNoMoreElement();
        }
    }

    @Override
    public List<RocketMqSourceSplit> snapshotState(long checkpointId) throws Exception {
        List<RocketMqSourceSplit> pendingSplit =
                sourceSplits.stream().map(RocketMqSourceSplit::copy).collect(Collectors.toList());
        Map<MessageQueue, Long> offsets =
                checkpointOffsets.computeIfAbsent(checkpointId, id -> Maps.newConcurrentMap());
        for (RocketMqSourceSplit split : pendingSplit) {
            offsets.put(split.getMessageQueue(), split.getStartOffset());
        }
        return pendingSplit;
    }

    @Override
    public void addSplits(List<RocketMqSourceSplit> splits) {
        running = true;
        splits.forEach(
                s -> {
                    try {
                        // 分片枚举器得到的分片分配到当前reader上，在这里加入到pendingPartitionsQueue阻塞队列中
                        pendingPartitionsQueue.put(s);
                    } catch (InterruptedException e) {
                        throw new RocketMqConnectorException(
                                RocketMqConnectorErrorCode.ADD_SPLIT_CHECKPOINT_FAILED, e);
                    }
                });
    }

    @Override
    public void handleNoMoreSplits() {
        // No-op
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (!checkpointOffsets.containsKey(checkpointId)) {
            log.warn("checkpoint {} do not exist or have already been committed.", checkpointId);
        } else {
            Map<MessageQueue, Long> messageQueueOffset = checkpointOffsets.remove(checkpointId);
            for (Map.Entry<MessageQueue, Long> entry : messageQueueOffset.entrySet()) {
                MessageQueue messageQueue = entry.getKey();
                Long offset = entry.getValue();
                try {
                    if (messageQueue != null && offset != null) {
                        consumerThreads
                                .get(messageQueue)
                                .getTasks()
                                .put(
                                        consumer -> {
                                            if (this.metadata.isEnabledCommitCheckpoint()) {
                                                consumer.getOffsetStore()
                                                        .updateOffset(messageQueue, offset, false);
                                                consumer.getOffsetStore().persist(messageQueue);
                                            }
                                        });
                    }
                } catch (InterruptedException e) {
                    log.error("commit offset failed", e);
                }
            }
        }
    }
}
