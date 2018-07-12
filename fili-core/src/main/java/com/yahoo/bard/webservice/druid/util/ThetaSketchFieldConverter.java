// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.util;

import com.yahoo.bard.webservice.druid.model.MetricField;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.aggregation.SketchAggregation;
import com.yahoo.bard.webservice.druid.model.aggregation.ThetaSketchAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.FuzzySetPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
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

    /**
     * Get the candidate Aggregation as a SketchEstimatePostAggregation.
     *
     * @param candidate  Aggregation to "convert"
     *
     * @return The Aggregation as a SketchEstimatePostAggregation
     */
    public ThetaSketchEstimatePostAggregation asSketchEstimate(Aggregation candidate) {
        String name = candidate.getName() + "_estimate";
        PostAggregation field = new FieldAccessorPostAggregation(candidate);
        return new ThetaSketchEstimatePostAggregation(name, field);
    }

    /**
     * Get the PostAggregation as a SketchEstimatePostAggregation.
     *
     * @param postAggregation  PostAggregation to "convert"
     *
     * @return The PostAggregation as a SketchEstimatePostAggregation
     */
    public ThetaSketchEstimatePostAggregation asSketchEstimate(PostAggregation postAggregation) {
        String name = postAggregation.getName() + "_estimate";
        return new ThetaSketchEstimatePostAggregation(name, postAggregation);
    }

    @Override
    public FuzzySetPostAggregation asSketchEstimate(MetricField field) {
        if (field.isSketch()) {
            if (field instanceof PostAggregation) {
                return asSketchEstimate((PostAggregation) field);
            }
            else if (field instanceof Aggregation) {
                return asSketchEstimate((Aggregation) field);
            } else {
                String message = "Given metric field " + field.toString() + " is neither a type of " +
                        "PostAggregation nor Aggregation";
                LOG.error(message);
                throw new IllegalArgumentException(message);
            }
        }
        String message = "Given metric field " + field.toString() + " isn't sketch";
        LOG.error(message);
        throw new IllegalArgumentException(message);
    }
}
