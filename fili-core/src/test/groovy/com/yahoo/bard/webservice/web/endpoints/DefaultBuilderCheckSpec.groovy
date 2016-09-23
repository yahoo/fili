// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import static com.yahoo.bard.webservice.config.BardFeatureFlag.INTERSECTION_REPORTING

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary

import spock.lang.Specification

import javax.ws.rs.core.Response

class DefaultBuilderCheckSpec extends Specification {
    boolean intersectionReportingState
    JerseyTestBinder jtb

    def setup() {
        intersectionReportingState = INTERSECTION_REPORTING.isOn()
        INTERSECTION_REPORTING.setOn(true)

        jtb = new JerseyTestBinder(DataServlet.class)

        DimensionDictionary dimensionStore = jtb.configurationLoader.dimensionDictionary

        dimensionStore.findByApiName("shape").with {
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "7665145", "shape1Desc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "52", "shape2Desc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "23", "shape3Desc"))
        }
    }

    def cleanup() {
        jtb.tearDown()
        INTERSECTION_REPORTING.setOn(intersectionReportingState)
    }

    String getTarget() {
        return "data/shapes/day/shape"
    }

    Map<String, List<String>> getQueryParams() {
        [
                "filters" : ["shape|id-in[7665145]"],
                "metrics" : ["users(AND(shape|id-in[52],shape|id-in[23]))"],
                "dateTime": ["2015-08-09%2F2015-08-10"]
        ]
    }

    /**
     * filterBuilder should be initialized before calling generateLogicalMetrics method in DataApiRequest constructor.
     * Because filterBuilder object is used to prepare Metric filters in case, metric/s contain filters for the
     * intersection reports.
     */
    def "test DataApiRequest member filterBuilder is initialized before generating filtered logical metric"() {

        when: "We send a request with intersection metric"
        Response response = makeAbstractRequest()

        then: "Successful status code is what we expect"
        response.getStatus() == 200
    }

    /**
     * Makes a request to the Druid backend.
     *
     * @param queryParams  A zero-argument closure that returns the query parameters as a
     * {@code Map&lt;String, List&lt;String>>} from a query parameter to a list of query parameter values that will be
     * joined by commas. Defaults to {@link BaseDataServletComponentSpec#getQueryParams()} if no argument is provided.
     *
     * @return The response from the Druid backend used by the harness
     */
    Response makeAbstractRequest(Closure queryParams=this.&getQueryParams) {
        int c = 0;
        // Set target of call
        def httpCall = jtb.getHarness().target(getTarget())

        // Add query params to call
        queryParams().each { String key, List<String> values ->
            httpCall = httpCall.queryParam(key, values.join(","))
        }
        // Make the call
        Response response = httpCall.request().get()
    }
}
