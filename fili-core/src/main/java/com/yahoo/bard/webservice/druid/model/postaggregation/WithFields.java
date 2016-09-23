// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.postaggregation;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.MetricField;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An Interface to getFields from a PostAggregation and return a PosAggregation constructed from the given fields.
 *
 * @param <T> a PostAggregation
 */
public interface WithFields<T extends PostAggregation> extends MetricField {
    /**
     * An interface method to get all the post aggregation fields.
     *
     * @return all the post aggregation fields when it is implemented
     */
     List<PostAggregation> getFields();

    /**
     * withFields provides new instance of respective class created from fields.
     *
     * @param fields  List of post aggregation fields
     *
     * @return new instance of implemented class created from the list of post aggregation fields
     */
     T withFields(List<PostAggregation> fields);

    /**
     * Get the dimensions from all child postAggregations.
     *
     * @return   The set of dimensions required to satisfy the fields of all child post aggregations
     */
    @Override
    @JsonIgnore
    default Set<Dimension> getDependentDimensions() {
        return getFields().stream()
                .map(MetricField::getDependentDimensions)
                .flatMap(Set::stream)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }
}
