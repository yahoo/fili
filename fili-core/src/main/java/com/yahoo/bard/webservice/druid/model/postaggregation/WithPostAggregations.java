// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.postaggregation;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.MetricField;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A MetricField that references MetricFields. Specific implementations of this method may further restrict the types or
 * amounts MetricFields they point to. Any further contract constraints on subclasses must be clearly documented.
 *
 * @param <T> Type of MetricField this MetricField references
 */
public interface WithPostAggregations<T extends PostAggregation> extends MetricField {

    /**
     * Returns all MetricFields this points to. The returned list is immutable.
     *
     * @return the immutable list of references MetricFields.
     */
    @JsonProperty("fields")
     List<PostAggregation> getPostAggregations();

    /**
     * Creates a copy of this WithFields implementations that replaces its referenced MetricFields with a copy of the
     * provided list of fields.
     *
     * @param fields  List of fields for the new WithFields instance to reference
     *
     * @return the copy
     */
     WithPostAggregations<T> withPostAggregations(List<? extends PostAggregation> fields);

    /**
     * Get the dimensions from all child MetricFields.
     *
     * @return the set of dimensions required to satisfy the fields of all child MetricField
     */
    @Override
    @JsonIgnore
    default Set<Dimension> getDependentDimensions() {
        return getPostAggregations().stream()
                .map(MetricField::getDependentDimensions)
                .flatMap(Set::stream)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }
}
