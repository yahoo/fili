// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.postaggregation;

import static com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation.DefaultPostAggregationType.THETA_SKETCH_ESTIMATE;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INVALID_NUMBER_OF_FIELDS;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Model representing post aggregation numeric estimates from sketches.
 */
public class ThetaSketchEstimatePostAggregation extends FuzzySetPostAggregation {

    private static final Logger LOG = LoggerFactory.getLogger(ThetaSketchEstimatePostAggregation.class);

    /**
     * Constructor accepting fields as list of field accessor post aggs.
     *
     * @param name  The name of post aggregation
     * @param field  The list of field accessor post aggs
     */
    public ThetaSketchEstimatePostAggregation(String name, PostAggregation field) {
        super(THETA_SKETCH_ESTIMATE, name, field);
    }

    @Override
    public ThetaSketchEstimatePostAggregation withName(String name) {
        return new ThetaSketchEstimatePostAggregation(name, getField());
    }

    @Override
    public ThetaSketchEstimatePostAggregation withField(PostAggregation field) {
        return new ThetaSketchEstimatePostAggregation(getName(), field);
    }

    /**
     * SketchEstimate converts the sketch into a number. Hence this method always should have one aggregator
     *
     * @param fields  List of post aggregation fields
     *
     * @return New ThetaSketchEstimatePostAggregation with provided field and only one aggregator.
     */
    @JsonIgnore
    @Override
    public ThetaSketchEstimatePostAggregation withFields(List<PostAggregation> fields) {
        if (fields.size() != 1) {
            LOG.error(INVALID_NUMBER_OF_FIELDS.logFormat(fields));
            throw new IllegalArgumentException(INVALID_NUMBER_OF_FIELDS.format(fields));
        }
        return withField(fields.get(0));
    }
}
