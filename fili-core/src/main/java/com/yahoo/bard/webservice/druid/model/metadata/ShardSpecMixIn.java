// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.metadata;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import io.druid.timeline.partition.NoneShardSpec;

/**
 * Defines a mix-in that is used to deserialize shard spec from json.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = NumberedShardSpec.class, name = "hashed"),
        @JsonSubTypes.Type(value = NumberedShardSpec.class, name = "linear"),
        @JsonSubTypes.Type(value = NumberedShardSpec.class, name = "numbered"),
        @JsonSubTypes.Type(value = NoneShardSpec.class, name = "none")
})
public abstract class ShardSpecMixIn { }
