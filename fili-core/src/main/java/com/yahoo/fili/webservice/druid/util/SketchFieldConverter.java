// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.druid.util;

import com.yahoo.fili.webservice.druid.model.MetricField;
import com.yahoo.fili.webservice.druid.model.aggregation.SketchAggregation;
import com.yahoo.fili.webservice.druid.model.aggregation.SketchCountAggregation;
import com.yahoo.fili.webservice.druid.model.aggregation.SketchMergeAggregation;
import com.yahoo.fili.webservice.druid.model.postaggregation.FieldAccessorPostAggregation;
import com.yahoo.fili.webservice.druid.model.postaggregation.FuzzySetPostAggregation;
import com.yahoo.fili.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.fili.webservice.druid.model.postaggregation.SketchEstimatePostAggregation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts Sketch fields (Aggregations and PostAggregations) to other Sketch fields.
 *
 * @deprecated  To consider the latest version of sketch Library.
 * This class is replaced by ThetaSketchFieldConverter class
 */
@Deprecated
public class SketchFieldConverter implements FieldConverters {

    private static final Logger LOG = LoggerFactory.getLogger(SketchFieldConverter.class);

    @Override
    public SketchCountAggregation asOuterSketch(SketchAggregation candidate) {
        return new SketchCountAggregation(candidate.getName(), candidate.getFieldName(), candidate.getSize());
    }

    @Override
    public SketchMergeAggregation asInnerSketch(SketchAggregation candidate) {
        return new SketchMergeAggregation(candidate.getName(), candidate.getFieldName(), candidate.getSize());
    }

    @Override
    public SketchEstimatePostAggregation asSketchEstimate(SketchAggregation candidate) {
        String name = candidate.getName() + "_estimate";
        PostAggregation field = new FieldAccessorPostAggregation(candidate);
        return new SketchEstimatePostAggregation(name, field);
    }

    @Override
    public FuzzySetPostAggregation asSketchEstimate(MetricField field) {
        if (field.isSketch()) {
            if (field instanceof PostAggregation) {
                PostAggregation pa = (PostAggregation) field;
                return new SketchEstimatePostAggregation(field.getName() + "_estimate", pa);
            } else {
                return asSketchEstimate((SketchAggregation) field);
            }
        }
        String message = "Given metric field " + field.toString() + " isn't sketch";
        LOG.error(message);
        throw new IllegalArgumentException(message);
    }
}
