// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers

import com.yahoo.bard.webservice.druid.model.aggregation.FilteredAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchSetOperationPostAggFunction
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchSetOperationPostAggregation
import com.yahoo.bard.webservice.web.SketchIntersectionReportingResources

import spock.lang.Specification

class SketchSetOperationHelperSpec extends Specification {

    SketchIntersectionReportingResources resources
    List<FilteredAggregation> filteredAggregationList
    SketchSetOperationPostAggregation resultPostAgg

    def setup() {
        resources = new SketchIntersectionReportingResources().init()

        filteredAggregationList = new ArrayList<>(resources.fooNoBarFilteredAggregationSet)
        filteredAggregationList.addAll(resources.fooRegFoosFilteredAggregationSet)

        resultPostAgg = SketchSetOperationHelper.makePostAggFromAgg(
                SketchSetOperationPostAggFunction.INTERSECT,
                "NEW_POST_AGG",
                filteredAggregationList
        )
    }

    def "makePostAggFromAgg returns a postAgg with the the correct set operation, name and fields"() {
        expect:
        resultPostAgg.getName() == "NEW_POST_AGG"
        resultPostAgg.getFunc().equals(SketchSetOperationPostAggFunction.INTERSECT)
        List<FieldAccessorPostAggregation> resultAggsList = new ArrayList<>()
        for (PostAggregation pa : resultPostAgg.getFields()) {
            FieldAccessorPostAggregation fa = (FieldAccessorPostAggregation) (pa);
            resultAggsList.add(fa.getAggregation())
        }
        resultAggsList.equals(filteredAggregationList)
    }
}
