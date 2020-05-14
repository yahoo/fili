// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.metric.antlr;


import static com.yahoo.bard.webservice.web.ErrorMessageFormat.METRIC_INVALID_WITH_DETAIL;

import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadMetricException;
import com.yahoo.bard.webservice.web.apirequest.generator.metric.ApiMetricParser;
import com.yahoo.bard.webservice.web.apirequest.metrics.ApiMetric;
import com.yahoo.bard.webservice.web.metrics.MetricsLex;
import com.yahoo.bard.webservice.web.metrics.MetricsParser;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class ProtocolAntlrApiMetricParser implements ApiMetricParser {
    private static final Logger LOG = LoggerFactory.getLogger(ProtocolAntlrApiMetricParser.class);

    @Override
    public List<ApiMetric> apply(
            String metricQuery
    ) throws BadApiRequestException {
        LOG.trace("Metrics query: {}", metricQuery);

        if (metricQuery == null || "".equals(metricQuery)) {
            return Collections.emptyList();
        }

        ApiMetricsListListener listener = new ApiMetricsListListener();
        MetricsLex lexer = MetricGrammarUtils.getLexer(metricQuery);
        MetricsParser parser = MetricGrammarUtils.getParser(lexer);
            try {
                MetricsParser.MetricsContext tree = parser.metrics();
                ParseTreeWalker.DEFAULT.walk(listener, tree);
            } catch (ParseCancellationException parseException) {
                LOG.debug(METRIC_INVALID_WITH_DETAIL.logFormat(metricQuery, parseException.getMessage()));
                throw new BadMetricException(
                        METRIC_INVALID_WITH_DETAIL.format(metricQuery, parseException.getMessage()),
                    parseException.getCause());
            }
            List<ApiMetric> generated = listener.getResults();
            LOG.trace("Generated list of metric detail: {}", generated);
            return generated;
    }
}
