// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.util;

import com.yahoo.bard.webservice.druid.model.MetricField;
import com.yahoo.bard.webservice.druid.model.aggregation.SketchAggregation;
import com.yahoo.bard.webservice.druid.model.aggregation.ThetaSketchAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.FuzzySetPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.ThetaSketchEstimatePostAggregation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts Sketch fields (Aggregations and PostAggregations) to other Sketch fields.
 */
public class ThetaSketchFieldConverter implements FieldConverters {

    private static final Logger LOG = LoggerFactory.getLogger(ThetaSketchFieldConverter.class);

    @Override
    public ThetaSketchAggregation asOuterSketch(SketchAggregation candidate) {
        return new ThetaSketchAggregation(candidate.getName(), candidate.getFieldName(), candidate.getSize());
    }

   @Override
    public  ThetaSketchAggregation asInnerSketch(SketchAggregation candidate) {
        return new ThetaSketchAggregation(candidate.getName(), candidate.getFieldName(), candidate.getSize());
    }

   @Override
    public ThetaSketchEstimatePostAggregation asSketchEstimate(SketchAggregation candidate) {
        String name = candidate.getName() + "_estimate";
        PostAggregation field = new FieldAccessorPostAggregation(candidate);
        return new ThetaSketchEstimatePostAggregation(name, field);
    }

    @Override
    public FuzzySetPostAggregation asSketchEstimate(MetricField field) {
        if (field.isSketch()) {
            if (field instanceof PostAggregation) {
                PostAggregation pa = (PostAggregation) field;
                return new ThetaSketchEstimatePostAggregation(field.getName() + "_estimate", pa);
            } else {
                return asSketchEstimate((SketchAggregation) field);
            }
        }
        String message = "Given metric field " + field.toString() + " isn't sketch";
        LOG.error(message);
        throw new IllegalArgumentException(message);
    }
}
