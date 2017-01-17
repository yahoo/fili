// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.parser;

import com.yahoo.bard.webservice.data.config.metric.antlrparser.FiliMetricLexer;
import com.yahoo.bard.webservice.data.config.metric.antlrparser.FiliMetricParser;
import com.yahoo.bard.webservice.data.config.provider.MakerBuilder;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;

/**
 * Parser used to build logical metrics from strings.
 *
 * Parser built using antlr4.
 */
public class FiliLogicalMetricParser {


    protected final ParseContext context;

    /**
     * Create a metric parser.
     *
     * @param metricName The metric name
     * @param metricDefinition The metric definition, as a string
     * @param dict the base metric dictionary
     * @param makerBuilder the metric maker builder
     * @param dimensionDictionary the dimension dictionary
     */
    public FiliLogicalMetricParser(
            String metricName,
            String metricDefinition,
            MetricDictionary dict,
            MakerBuilder makerBuilder,
            DimensionDictionary dimensionDictionary
    ) {
        this.context = new ParseContext(
                metricName,
                metricDefinition,
                dict,
                makerBuilder,
                dimensionDictionary
        );
    }

    /**
     * Parses the metric def and returns a LogicalMetric.
     *
     * Note: caller should add to MetricDictionary as parse() use a scoped dictionary.
     *
     * @return a logical metric
     */
    public LogicalMetric parse() {
        CharStream charStream = new ANTLRInputStream(this.context.metricDefinition);
        FiliMetricLexer lexer = new FiliMetricLexer(charStream);
        TokenStream tokens = new CommonTokenStream(lexer);
        FiliMetricParser parser = new FiliMetricParser(tokens);

        FiliExpressionVisitor visitor = new FiliExpressionVisitor(context);
        return visitor.visitFiliExpression(parser.filiExpression());
    }

}
