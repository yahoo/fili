// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.metric.antlr;

import com.yahoo.bard.webservice.web.apirequest.generator.having.ExceptionErrorListener;
import com.yahoo.bard.webservice.web.metrics.MetricsLex;
import com.yahoo.bard.webservice.web.metrics.MetricsParser;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;

public class MetricGrammarUtils {

    public static MetricsLex getLexer(String havingQuery) {
        ANTLRInputStream input = new ANTLRInputStream(havingQuery);
        MetricsLex lexer = new MetricsLex(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(ExceptionErrorListener.INSTANCE);
        return lexer;
    }
    public static MetricsParser getParser(MetricsLex lexer) {
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        MetricsParser parser = new MetricsParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(ExceptionErrorListener.INSTANCE);
        return parser;
    }

    public static String resolveEscapedString(String escapedString) {
        String localCopy = escapedString;
        // remove start and end quote
        localCopy = localCopy.substring(1, localCopy.length() - 1);
        // TODO replace this with a precompiled pattern
        localCopy = localCopy.replace("\\)", ")");
        return localCopy;
    }
}
