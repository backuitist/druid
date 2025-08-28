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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskClientFactory;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.pulsar.client.api.MessageId;

@LazySingleton
public class PulsarIndexTaskClientFactory extends SeekableStreamIndexTaskClientFactory<Integer, MessageId>
{
  @Inject
  public PulsarIndexTaskClientFactory(
      @EscalatedGlobal HttpClient httpClient,
      @Json ObjectMapper mapper
  )
  {
      super(httpClient, mapper);
    }

    @Override
    public Class<Integer> getPartitionType()
    {
      return Integer.class;
    }

    @Override
    public Class<MessageId> getSequenceType()
    {
      return MessageId.class;
    }
}
