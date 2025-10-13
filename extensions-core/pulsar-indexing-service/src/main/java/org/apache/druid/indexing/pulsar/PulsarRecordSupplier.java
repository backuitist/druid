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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.TopicName;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;

// used by both the supervisor and the index tasks
public class PulsarRecordSupplier implements RecordSupplier<Integer, MessageId, PulsarRecordEntity>
{
  private static final Logger log = new Logger(PulsarRecordSupplier.class);
  private volatile Reader<GenericRecord> reader;
  private volatile Set<StreamPartition<Integer>> assignment = Set.of();
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
        log.info("Reassigning reader (old assignment = " + assignment.toString() + ")");
        reader.closeAsync();
      }

      if (streamPartitions.isEmpty()) {
        log.info("Assigning nothing.");
        reader = null;
        assignment = streamPartitions;
        return;
      }

      log.info("Assigning partitions: " + streamPartitions);

      String partitionsId = streamPartitions.stream().map(st -> st.getPartitionId().toString()).sorted().collect(joining("-"));
      reader = client.newReader(Schema.AUTO_CONSUME())
              .readerName(readerName + "-" + partitionsId)
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
    failOnUnknownPartition(partition);
    try {
      log.info("Seeking %s to %s", partition, sequenceNumber);
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
    if (reader == null) {
      throw new ISE("Cannot poll: no assignment available.");
    }
    try {
      List<OrderedPartitionableRecord<Integer, MessageId, PulsarRecordEntity>> records = new ArrayList<>();

      Message<GenericRecord> item = reader.readNext((int) timeout, TimeUnit.MILLISECONDS);

      if (item == null) {
        return records;
      }

      MessageId currentId = item.getMessageId();
      MessageId minId = currentId;
      MessageId maxId = currentId;

      int numberOfRecords = 0;

      while (item != null) {
        currentId = item.getMessageId();
        if (currentId.compareTo(minId) < 0) {
          minId = currentId;
        }
        if (currentId.compareTo(maxId) > 0) {
          maxId = currentId;
        }
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

      log.info("Polled records: [%s; %s]", minId, maxId);
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
    // This method is used by the supervisor when creating ingestion tasks. Spinning up a consumer is a bit expensive
    // but shouldn't be too frequent.
    // I don't think there are any other ways to check offset availability: the admin client can tell you which is the
    // latest but not the earlier offet of a given topic.
    try(Consumer<byte[]> sub = client.newConsumer().topic(TopicName.getTopicPartitionNameString(partition.getStream(), partition.getPartitionId()))
            .subscriptionType(SubscriptionType.Exclusive)
            .subscriptionName("record-supplier-is-offset-avail_" + UUID.randomUUID())
            .subscribe()) {
      try {
        sub.seek(offset.get());
        // seek succeeded, offset is available
        return true;
      } catch (PulsarClientException e) {
        // cannot seek
        return false;
      }
    } catch (PulsarClientException e) {
        throw new RuntimeException("Failed to determine if offset " + offset + " is available on " + partition, e);
    }
  }

  @Override
  public MessageId getPosition(StreamPartition<Integer> partition)
  {
    failOnUnknownPartition(partition);
    try {
      List<TopicMessageId> lastMessageIds = reader.getLastMessageIds();
      TopicMessageId topicMessageId = lastMessageIds.stream().filter(t -> t.getOwnerTopic().equals(getTopicFromStreamPartition(partition)))
              .findFirst().orElseThrow(() -> new IllegalArgumentException("Cannot get position of "+ partition + " - where we are not assigned " + lastMessageIds.stream().map(t -> t.getOwnerTopic()).collect(Collectors.toList())));
      return topicMessageId;
    } catch (PulsarClientException e) {
        throw new StreamException(e);
    }
  }

  private void failOnUnknownPartition(StreamPartition<Integer> partition) {
    if (!assignment.contains(partition)) {
      throw new ISE("Partition [%s] hasn't been assigned", partition);
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
    if (reader != null) {
      reader.closeAsync();
    }
    client.closeAsync();
  }
}
