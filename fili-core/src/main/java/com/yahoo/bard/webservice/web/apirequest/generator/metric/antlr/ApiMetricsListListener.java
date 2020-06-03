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

    private static final String MISSING_VALUE_FORMAT = "No recognized parsed value for parameter %s";

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
        // TODO strip escaping characters if present AND remove any escape characters
        String paramName = ctx.ID().getText();
        String value;
        if (ctx.VALUE() != null) {
            value = ctx.VALUE().getText();
        } else if (ctx.ESCAPED_VALUE() != null) {
            value = MetricGrammarUtils.resolveEscapedString(ctx.ESCAPED_VALUE().getText());
        } else {
            throw new IllegalStateException(String.format(MISSING_VALUE_FORMAT, paramName));
        }
        paramsSoFar.put(paramName, value);


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
