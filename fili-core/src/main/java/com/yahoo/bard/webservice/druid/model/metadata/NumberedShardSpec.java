// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Range;

import org.apache.commons.lang3.NotImplementedException;

import io.druid.data.input.InputRow;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.ShardSpec;
import io.druid.timeline.partition.ShardSpecLookup;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * NumberedShardSpec class. Reflects the current shardspec type that is used in druid datasource metadata endpoints.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class NumberedShardSpec implements ShardSpec {

    private final String type;
    private final int partitionNum;
    private final int partitions;

    /**
     * Creates a numbered shard specification given a type, a partition number and the total number of partitions.
     *
     * @param type  The type of this shard spec.
     * @param partitionNum  The partition number of this shard spec.
     * @param partitions  The total number of partitions of the segment that this shard spec belongs to.
     */
    @JsonCreator
    public NumberedShardSpec(
            @JsonProperty("type") String type,
            @JsonProperty("partitionNum") int partitionNum,
            @JsonProperty("partitions") int partitions
    ) {
        this.type = type;
        this.partitionNum = partitionNum;
        this.partitions = partitions;
    }

    /**
     * Creates numbered shard spec from an unsharded spec.
     * Consequently the numbered shard spec will have type: "none", partition number equal to zero and number of
     * partitions equal to one.
     *
     * @param spec  The spec corresponding to unsharded segment.
     */
    public NumberedShardSpec(NoneShardSpec spec) {
        this.type = "none";
        this.partitionNum = spec.getPartitionNum();
        this.partitions = this.partitionNum + 1;
    }

    /**
     * Getter for type.
     *
     * @return type  The type of this shard spec.
     */
    public String getType() {
        return this.type;
    }

    @Override
    public <T> PartitionChunk<T> createChunk(T obj) {
        throw new NotImplementedException("createChunk method is not implemented");
    }

    @Override
    public boolean isInChunk(long timestamp, InputRow inputRow) {
        throw new NotImplementedException("isInChunk method is not implemented");
    }

    /**
     * Getter for partition number.
     *
     * @return The partition number of this shard spec.
     */
    @Override
    public int getPartitionNum() {
        return partitionNum;
    }

    @Override
    public ShardSpecLookup getLookup(List<ShardSpec> shardSpecs) {
        throw new NotImplementedException("getLookup method is not implemented");
    }

    /**
     * Getter for the number of partitions.
     *
     * @return The number of partitions of the segment that this shard belongs to.
     */
    public int getPartitions() {
        return partitions;
    }

    /**
     * Get the possible range of each dimension for the rows this shard contains.
     *
     * @return map of dimensions to its possible range. Dimensions with unknown possible range are not mapped
     */
    public Map<String, Range<String>> getDomain() {
        return Collections.emptyMap();
    }
}
