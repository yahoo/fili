// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.util;

import com.yahoo.bard.webservice.druid.model.MetricField;
import com.yahoo.bard.webservice.druid.model.aggregation.SketchAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.FuzzySetPostAggregation;

/**
 * An interface for transforming sketch aggregations into other sketch (post-)aggregations.
 */
public interface FieldConverters {

    /**
     * Get the outer SketchAggregation when nesting.
     *
     * @param candidate  SketchAggregation to "convert"
     *
     * @return The SketchAggregation
     */
    SketchAggregation asOuterSketch(SketchAggregation candidate);

    /**
     * Get the inner SketchAggregation when nesting.
     *
     * @param candidate  SketchAggregation to "convert"
     *
     * @return The SketchAggregation
     */
    SketchAggregation asInnerSketch(SketchAggregation candidate);

    /**
     * Get the candidate SketchAggregation as a ThetaSketchEstimatePostAggregation.
     *
     * @param candidate  SketchAggregation to "convert"
     *
     * @return The SketchAggregation as a ThetaSketchEstimatePostAggregation
     */
    FuzzySetPostAggregation asSketchEstimate(SketchAggregation candidate);

    /**
     * Get the candidate MetricField as a PostAggregation.
     *
     * @param field  Metric field which can be aggregate or post aggregate
     *
     * @return  The SketchPostEstimateAggregation based on field
     */
    FuzzySetPostAggregation asSketchEstimate(MetricField field);
}
