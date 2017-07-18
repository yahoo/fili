// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.druid.model.postaggregation;

import static com.yahoo.fili.webservice.druid.model.postaggregation.PostAggregation.DefaultPostAggregationType.SKETCH_ESTIMATE;
import static com.yahoo.fili.webservice.web.ErrorMessageFormat.INVALID_NUMBER_OF_FIELDS;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Model representing post aggregation numeric estimates from sketches.
 *
 * @deprecated  To consider the latest version of sketch Library.
 * This class is replaced by ThetaSketchEstimatePostAggregation class
 */
@Deprecated
public class SketchEstimatePostAggregation extends FuzzySetPostAggregation {

    private static final Logger LOG = LoggerFactory.getLogger(SketchEstimatePostAggregation.class);

    /**
     * Constructor accepting fields as list of field accessor post aggs.
     *
     * @param name  The name of post aggregation
     * @param field  The list of field accessor post aggs
     */
    public SketchEstimatePostAggregation(String name, PostAggregation field) {
        super(SKETCH_ESTIMATE, name, field);
    }

    @Override
    public SketchEstimatePostAggregation withName(String name) {
        return new SketchEstimatePostAggregation(name, getField());
    }

    @Override
    public SketchEstimatePostAggregation withField(PostAggregation field) {
        return new SketchEstimatePostAggregation(getName(), field);
    }

    /**
     * SketchEstimate converts the sketch into a number. Hence this method always should have one aggregator
     *
     * @param fields  List of post aggregation fields
     *
     * @return New SketchEstimatePostAggregation with provided field and only one aggregator.
     */
    @JsonIgnore
    @Override
    public SketchEstimatePostAggregation withFields(List<PostAggregation> fields) {
        if (fields.size() != 1) {
            LOG.error(INVALID_NUMBER_OF_FIELDS.logFormat(fields));
            throw new IllegalArgumentException(INVALID_NUMBER_OF_FIELDS.format(fields));
        }
        return withField(fields.get(0));
    }
}
