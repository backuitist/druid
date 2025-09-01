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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class PulsarRecordSupplier implements RecordSupplier<Integer, MessageId, PulsarRecordEntity>, ReaderListener<GenericRecord>
{
  private static final Logger log = new Logger(PulsarRecordSupplier.class);
  private final ConcurrentHashMap<StreamPartition<Integer>, Container> readers = new ConcurrentHashMap<>();
  private final PulsarClient client;
  private PulsarClientException previousSeekFailure;
  private final Integer maxRecordsInSinglePoll;
  private final BlockingQueue<Message<GenericRecord>> received;

  protected final String readerName;

  public PulsarRecordSupplier(ClientConfigurationData pulsarClientConf,
                              String readerName,
                              Integer maxRecordsInSinglePoll)
  {
    this.readerName = readerName;
    this.maxRecordsInSinglePoll = maxRecordsInSinglePoll;
    this.received = new ArrayBlockingQueue<>(this.maxRecordsInSinglePoll);

    try {
      this.client = new PulsarClientImpl(pulsarClientConf);
    }
    catch (PulsarClientException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void assign(Set<StreamPartition<Integer>> streamPartitions)
  {
    List<CompletableFuture<Reader<GenericRecord>>> futures = new ArrayList<>();
    log.info("Assigning partitions: " + streamPartitions);

    try {
      for (StreamPartition<Integer> partition : streamPartitions) {
        if (readers.containsKey(partition)) {
          continue;
        }

        String topic = TopicName.get(partition.getStream())
                                .getPartition(partition.getPartitionId())
                                .toString();

        futures.add(buildConsumer(client, topic).thenApplyAsync(reader -> {
          if (readers.containsKey(partition)) {
            reader.closeAsync();
          } else {
            readers.put(partition, new Container(reader, MessageId.earliest));
          }
          return reader;
        }));
      }

      futures.forEach(CompletableFuture::join);
    }
    catch (Exception e) {
      futures.forEach(f -> {
        try {
          f.get().closeAsync();
        }
        catch (Exception ignored) {
          // ignore
        }
      });
      throw new StreamException(e);
    }
    log.info("Successfully assigned: " + streamPartitions);
  }

  public PulsarClientException getPreviousSeekFailure()
  {
    return previousSeekFailure;
  }

  @Override
  public void seek(StreamPartition<Integer> partition, MessageId sequenceNumber) throws InterruptedException
  {
    Container reader = readers.get(partition);
    if (reader == null) {
      throw new IllegalArgumentException("Cannot seek on a partition where we are not assigned");
    }

    try {
      reader.reader.seek(sequenceNumber);
      setPosition(partition, sequenceNumber);
      previousSeekFailure = null;
    }
    catch (PulsarClientException e) {
      previousSeekFailure = e;
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


      Message<GenericRecord> item = received.poll(timeout, TimeUnit.MILLISECONDS);
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

        setPosition(sp, item.getMessageId());

        if (++numberOfRecords >= maxRecordsInSinglePoll) {
          break;
        }

        // Check if we have an item already available
        item = received.poll(0, TimeUnit.MILLISECONDS);
      }

      return records;
    }
    catch (InterruptedException e) {
      throw new StreamException(e);
    }
  }

  @Override
  public Collection<StreamPartition<Integer>> getAssignment()
  {
    log.info("getAssignment: " + readers.keySet());
    return this.readers.keySet();
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
    Container reader = readers.get(partition);
    if (reader == null) {
      throw new IllegalArgumentException("Cannot seek on a partition where we are not assigned");
    }
    return reader.position;
  }

  @Override
  public Set<Integer> getPartitionIds(String stream)
  {
    try {
      return client.getPartitionsForTopic(stream).get().stream()
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
    readers.forEach((k, r) -> r.reader.closeAsync());
    client.closeAsync();
  }

  void setPosition(StreamPartition<Integer> partition, MessageId position)
  {
    Container reader = this.readers.get(partition);
    if (reader != null) {
      reader.position = position;
    }
  }

  CompletableFuture<Reader<GenericRecord>> buildConsumer(PulsarClient client, String topic)
  {
    return client.newReader(Schema.AUTO_CONSUME())
                 .readerName(readerName)
                 .topic(topic)
                 .readerListener(this)
                 .startMessageId(MessageId.earliest)
                 .createAsync();
  }

  @Override
  public void received(Reader<GenericRecord> reader, Message<GenericRecord> message)
  {
    try {
      this.received.put(message);
    }
    catch (InterruptedException e) {
      throw new StreamException(e);
    }
  }

  @Override
  public void reachedEndOfTopic(Reader<GenericRecord> reader) {
    // no-op
  }

  public static class Container
  {
    public Reader<GenericRecord> reader;
    public MessageId position;

    public Container(Reader<GenericRecord> reader, MessageId position)
    {
      this.reader = reader;
      this.position = position;
    }
  }
}
