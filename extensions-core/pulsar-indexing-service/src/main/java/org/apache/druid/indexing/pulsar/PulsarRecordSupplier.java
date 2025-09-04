/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.pulsar;

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.pulsar.PulsarRecordEntity;
import org.apache.druid.indexing.seekablestream.common.*;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.TopicName;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class PulsarRecordSupplier implements RecordSupplier<Integer, MessageId, PulsarRecordEntity>
{
  private static final Logger log = new Logger(PulsarRecordSupplier.class);
  private Reader<GenericRecord> reader;
  private Set<StreamPartition<Integer>> assignment = Set.of();
  private final PulsarClient client;
  private final Integer maxRecordsInSinglePoll;

  protected final String readerName;

  public PulsarRecordSupplier(ClientConfigurationData pulsarClientConf,
                              String readerName,
                              Integer maxRecordsInSinglePoll)
  {
    this.readerName = readerName;
    this.maxRecordsInSinglePoll = maxRecordsInSinglePoll;

    try {
      this.client = new PulsarClientImpl(pulsarClientConf);
    }
    catch (PulsarClientException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  synchronized public void assign(Set<StreamPartition<Integer>> streamPartitions)
  {
    try {
      if (reader != null) {
        // this isn't supposed to happen I believe?
        log.info("Reassigning reader (old assignment = " + assignment.toString() + ") -> " + streamPartitions);
        reader.closeAsync();
      } else {
        log.info("Assigning partitions: " + streamPartitions);
      }
      reader = client.newReader(Schema.AUTO_CONSUME())
              .readerName(readerName)
              .topics(streamPartitions.stream().map(StreamPartition::getStream)
                      .collect(Collectors.toUnmodifiableList()))
              .startMessageId(MessageId.earliest)
              .create();
      assignment = streamPartitions;
    }
    catch (IOException e) {
      throw new StreamException(e);
    }
    log.info("Successfully assigned: " + streamPartitions);
  }

  @Override
  public void seek(StreamPartition<Integer> partition, MessageId sequenceNumber) throws InterruptedException
  {
    try {
      reader.seek(topic -> {
        if (topic.equals(getTopicFromStreamPartition(partition))) {
          return sequenceNumber;
        } else { return null; }
      });
    }
    catch (PulsarClientException e) {
      throw new StreamException(e);
    }
  }

  @Override
  public void seekToEarliest(Set<StreamPartition<Integer>> streamPartitions)
  {
    streamPartitions.forEach(p -> {
      try {
        seek(p, MessageId.earliest);
      }
      catch (InterruptedException e) {
        throw new StreamException(e);
      }
    });
  }

  @Override
  public void seekToLatest(Set<StreamPartition<Integer>> streamPartitions)
  {
    streamPartitions.forEach(p -> {
      try {
        seek(p, MessageId.latest);
      }
      catch (InterruptedException e) {
        throw new StreamException(e);
      }
    });

  }

  private StreamPartition<Integer> getStreamPartitionFromMessage(Message<GenericRecord> msg)
  {
    TopicName topic = TopicName.get(msg.getTopicName());
    return new StreamPartition<>(topic.getPartitionedTopicName(), topic.getPartitionIndex());
  }

  @Override
  public List<OrderedPartitionableRecord<Integer, MessageId, PulsarRecordEntity>> poll(long timeout)
  {
    try {
      List<OrderedPartitionableRecord<Integer, MessageId, PulsarRecordEntity>> records = new ArrayList<>();

      Message<GenericRecord> item = reader.readNext((int) timeout, TimeUnit.MILLISECONDS);

      if (item == null) {
        return records;
      }

      int numberOfRecords = 0;

      while (item != null) {
        StreamPartition<Integer> sp = getStreamPartitionFromMessage(item);

        records.add(new OrderedPartitionableRecord<>(
            sp.getStream(),
            sp.getPartitionId(),
            item.getMessageId(),
            ImmutableList.of(new PulsarRecordEntity(item))
        ));

        if (++numberOfRecords >= maxRecordsInSinglePoll) {
          break;
        }

        // Check if we have an item already available
        item = reader.readNext(0, TimeUnit.MILLISECONDS);
      }

      return records;
    }
    catch (PulsarClientException e) {
      throw new StreamException(e);
    }
  }

  @Override
  public Collection<StreamPartition<Integer>> getAssignment()
  {
    return assignment;
  }

  @Nullable
  @Override
  public MessageId getLatestSequenceNumber(StreamPartition<Integer> partition)
  {
    return MessageId.latest;
  }

  @Nullable
  @Override
  public MessageId getEarliestSequenceNumber(StreamPartition<Integer> partition)
  {
    return MessageId.earliest;
  }

  @Override
  public boolean isOffsetAvailable(StreamPartition<Integer> partition, OrderedSequenceNumber<MessageId> offset) {
    return false;
  }

  @Override
  public MessageId getPosition(StreamPartition<Integer> partition)
  {
      try {
        List<TopicMessageId> lastMessageIds = reader.getLastMessageIds();
        TopicMessageId topicMessageId = lastMessageIds.stream().filter(t -> t.getOwnerTopic().equals(getTopicFromStreamPartition(partition)))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("Cannot get position of "+ partition + " - where we are not assigned " + lastMessageIds.stream().map(t -> t.getOwnerTopic()).collect(Collectors.toList())));
        return topicMessageId;
      } catch (PulsarClientException e) {
          throw new StreamException(e);
      }
  }

  private String getTopicFromStreamPartition(StreamPartition<Integer> partition) {
    return partition.getStream() + "-partition-" + partition.getPartitionId();
  }

  @Override
  public Set<Integer> getPartitionIds(String stream)
  {
    try {
      // TODO (BBT) what if metadata auto creation isn't enabled? would that fail when a topic isn't "initialized"?
      //            looks like an edge case, not going to bother for now
      return client.getPartitionsForTopic(stream, true).get().stream()
                   .map(TopicName::get)
                   .map(TopicName::getPartitionIndex)
                   .collect(Collectors.toSet());
    }
    catch (Exception e) {
      throw new StreamException(e);
    }
  }

  @Override
  public void close()
  {
    reader.closeAsync();
    client.closeAsync();
  }
}
