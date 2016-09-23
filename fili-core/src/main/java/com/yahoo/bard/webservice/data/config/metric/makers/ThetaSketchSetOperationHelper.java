// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.druid.model.aggregation.FilteredAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchSetOperationPostAggFunction;
import com.yahoo.bard.webservice.druid.model.postaggregation.ThetaSketchSetOperationPostAggregation;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A Helper class for ThetaSketchSetOperationMaker that builds a ThetaSketchSetOperationPostAggregation
 * from the intersection of filteredAggregators.
 */
public class ThetaSketchSetOperationHelper {

    /**
     * This method builds a ThetaSketchSetOperationPostAggregation by applying the set operation specified by 'function'
     * on the FilteredAggregations in the filteredAggregationList.
     *
     * @param function  The set operation to perform. ex: UNION, INTERSECT
     * @param name  The name for the resultant SketchSetOperationPostAggregation
     * @param filteredAggregationList  A List containing FilteredAggregators that should be used to generate the
     * ThetaSketchSetOperationPostAggregation
     *
     * @return  A ThetaSketchSetOperationPostAggregation, that is obtained by applying the set operation specified by
     * 'function' on the FilteredAggregations in the filteredAggregationList
     */
    public static ThetaSketchSetOperationPostAggregation makePostAggFromAgg(
            SketchSetOperationPostAggFunction function,
            String name,
            List<FilteredAggregation> filteredAggregationList
    ) {
        //iterate filteredAggregationMap
        List<PostAggregation> operands = filteredAggregationList.stream()
                .map(FieldAccessorPostAggregation::new)
                .collect(Collectors.toList());
        return new ThetaSketchSetOperationPostAggregation(name, function, operands);
    }
}
