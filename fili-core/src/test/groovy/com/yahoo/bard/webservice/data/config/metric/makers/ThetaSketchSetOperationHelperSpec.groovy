// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers

import com.yahoo.bard.webservice.druid.model.aggregation.FilteredAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchSetOperationPostAggFunction
import com.yahoo.bard.webservice.druid.model.postaggregation.ThetaSketchSetOperationPostAggregation
import com.yahoo.bard.webservice.web.ThetaSketchIntersectionReportingResources

import spock.lang.Specification

class ThetaSketchSetOperationHelperSpec extends Specification {

    ThetaSketchIntersectionReportingResources resources
    List<FilteredAggregation> filteredAggregationList
    ThetaSketchSetOperationPostAggregation resultPostAgg

    def setup() {
        resources = new ThetaSketchIntersectionReportingResources().init()

        filteredAggregationList = new ArrayList<>(resources.fooNoBarFilteredAggregationSet)
        filteredAggregationList.addAll(resources.fooRegFoosFilteredAggregationSet)

    }

    def "makePostAggFromAgg returns a postAgg with the the correct set operation, name and fields"() {

        given:
        resultPostAgg = ThetaSketchSetOperationHelper.makePostAggFromAgg(
                SketchSetOperationPostAggFunction.INTERSECT,
                "NEW_POST_AGG",
                filteredAggregationList
        )

        expect:
        resultPostAgg.getName() == "NEW_POST_AGG"
        resultPostAgg.getFunc() == SketchSetOperationPostAggFunction.INTERSECT
        List<FieldAccessorPostAggregation> resultAggsList = new ArrayList<>()
        for (PostAggregation pa : resultPostAgg.getFields()) {
            FieldAccessorPostAggregation fa = (FieldAccessorPostAggregation) pa;
            resultAggsList.add(fa.getAggregation())
        }
        resultAggsList == filteredAggregationList
    }
}
