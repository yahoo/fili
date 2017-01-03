// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.druid.model.aggregation.FilteredAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchSetOperationPostAggFunction;
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchSetOperationPostAggregation;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A Helper class for SketchSetOperation that builds a SketchSetOperationPostAggregation from the intersection of
 * filteredAggregators.
 *
 * @deprecated  To consider the latest version of sketch Library.
 * This class is replaced by ThetaSketchSetOperationHelper
 */
@Deprecated
public class SketchSetOperationHelper {

    /**
     * This method builds a SketchSetOperationPostAggregation by applying the set operation specified by 'function'
     * on the FilteredAggregations in the filteredAggregationList.
     *
     * @param function  The set operation to perform. ex: UNION, INTERSECT
     * @param name  The name for the resultant SketchSetOperationPostAggregation
     * @param filteredAggregationList  A List containing FilteredAggregators that should be used to generate the
     * SketchSetOperationPostAggregation
     *
     * @return  A SketchSetOperationPostAggregation, that is obtained by applying the set operation specified by
     * 'function' on the FilteredAggregations in the filteredAggregationList
     */
    public static SketchSetOperationPostAggregation makePostAggFromAgg(
            SketchSetOperationPostAggFunction function,
            String name,
            List<FilteredAggregation> filteredAggregationList
    ) {
        List<PostAggregation> operands = filteredAggregationList.stream()
                .map(FieldAccessorPostAggregation::new)
                .collect(Collectors.toList());
        return new SketchSetOperationPostAggregation(name, function, operands);
    }
}
