package com.yahoo.bard.webservice.web.apirequest.generator;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder;
import com.yahoo.bard.webservice.web.apirequest.RequestParameters;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import java.util.LinkedHashSet;

public class DefaultLogicalMetricGenerator implements Generator<LinkedHashSet<LogicalMetric>> {
    @Override
    public LinkedHashSet<LogicalMetric> bind(
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        return null;
    }

    @Override
    public void validate(
            LinkedHashSet<LogicalMetric> entity,
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {

    }
}
