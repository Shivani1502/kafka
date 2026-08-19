/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MirrorSourceTaskOffsetLossTest {

    @Test
    public void testPollFailsFastWhenSourceRecordsWerePurged() {
        TopicPartition topicPartition = new TopicPartition("source-topic", 0);
        long requestedOffset = 7L;

        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        when(consumer.poll(any())).thenThrow(
                new OffsetOutOfRangeException(Map.of(topicPartition, requestedOffset)));
        when(consumer.beginningOffsets(Set.of(topicPartition))).thenReturn(Map.of(topicPartition, 10L));

        MirrorSourceTask task = new MirrorSourceTask(consumer, null, "source",
                new DefaultReplicationPolicy(), null, true);

        assertThrows(DataLossException.class, task::poll);
    }

    @Test
    public void testPollFailsFastWhenSourceTopicWasReset() {
        TopicPartition topicPartition = new TopicPartition("source-topic", 0);
        long requestedOffset = 42L;

        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        when(consumer.poll(any())).thenThrow(
                new OffsetOutOfRangeException(Map.of(topicPartition, requestedOffset)));
        when(consumer.beginningOffsets(Set.of(topicPartition))).thenReturn(Map.of(topicPartition, 0L));

        MirrorSourceTask task = new MirrorSourceTask(consumer, null, "source",
                new DefaultReplicationPolicy(), null, true);

        assertThrows(TopicResetException.class, task::poll);
    }

    @Test
    public void testOffsetOutOfRangeKeepsExistingBehaviorWhenValidationDisabled() {
        TopicPartition topicPartition = new TopicPartition("source-topic", 0);

        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        when(consumer.poll(any())).thenThrow(new OffsetOutOfRangeException(Map.of(topicPartition, 7L)));

        MirrorSourceTask task = new MirrorSourceTask(consumer, null, "source",
                new DefaultReplicationPolicy(), null, false);

        assertNull(task.poll());
        verify(consumer, never()).beginningOffsets(any());
    }

    @Test
    public void testUncommittedPartitionSeeksToBeginningInFailFastMode() {
        TopicPartition topicPartition = new TopicPartition("new-source-topic", 1);

        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        SourceTaskContext sourceTaskContext = mock(SourceTaskContext.class);
        OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
        when(sourceTaskContext.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offset(anyMap())).thenReturn(null);

        MirrorSourceTask task = new MirrorSourceTask(consumer, null, "source",
                new DefaultReplicationPolicy(), null, true);
        task.initialize(sourceTaskContext);

        task.initializeConsumer(Set.of(topicPartition));

        verify(consumer).assign(Set.of(topicPartition));
        verify(consumer).seekToBeginning(Set.of(topicPartition));
    }
}
