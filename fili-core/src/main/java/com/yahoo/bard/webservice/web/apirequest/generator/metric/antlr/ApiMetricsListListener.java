// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.metric.antlr;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadMetricException;
import com.yahoo.bard.webservice.web.apirequest.metrics.ApiMetric;
import com.yahoo.bard.webservice.web.metrics.MetricsBaseListener;
import com.yahoo.bard.webservice.web.metrics.MetricsParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ApiMetricsListListener extends MetricsBaseListener {

    private final Map<String, LogicalMetric> metrics = new HashMap<>();

    private Map<String, String> paramsSoFar = new HashMap<>();
    private List<ApiMetric> results = new ArrayList<>();

    private Exception error = null;

    @Override
    public void enterMetric(MetricsParser.MetricContext ctx) {
        paramsSoFar = new HashMap<>();
    }

    @Override
    public void exitMetric(MetricsParser.MetricContext ctx) {
        final String metricApiText = ctx.getText();
        final String baseMetricId = ctx.metricName().ID().getText();
        ApiMetric metricDetail = new ApiMetric(metricApiText, baseMetricId, paramsSoFar);
        results.add(metricDetail);
    }

    @Override
    public void exitParamValue(MetricsParser.ParamValueContext ctx) {
        // TODO strip quotes if present AND remove any escape characters
        String value;
        if (ctx.VALUE() != null) {
            value = ctx.VALUE().getText();
        } else {
            String quotedParamValue = ctx.QUOTED_VALUE().getText();
            // remove start and end quote
            quotedParamValue = quotedParamValue.substring(1, quotedParamValue.length() - 1);
            // TODO replace this with a precompiled pattern
            quotedParamValue = quotedParamValue.replace("\\'", "'");
            value = quotedParamValue;
        }
        paramsSoFar.put(ctx.ID().getText(), value);


    }

    /**
     * Throw any pending exceptions.
     *
     * @throws BadMetricException Thrown when the having is invalid
     */
    protected void processErrors() throws BadMetricException {

        if (error != null) {
            throw (BadMetricException) error;
        }
    }
    public List<ApiMetric> getResults() {
        return results;
    }
}
