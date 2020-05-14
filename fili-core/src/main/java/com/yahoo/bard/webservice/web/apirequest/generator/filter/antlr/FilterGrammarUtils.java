// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.filter.antlr;

import com.yahoo.bard.webservice.web.apirequest.generator.having.ExceptionErrorListener;
import com.yahoo.bard.webservice.web.filters.FiltersLex;
import com.yahoo.bard.webservice.web.filters.FiltersParser;
import com.yahoo.bard.webservice.web.metrics.MetricsLex;
import com.yahoo.bard.webservice.web.metrics.MetricsParser;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;

//TODO this can PROBABLY be refactored into more general util class
public class FilterGrammarUtils {

    public static FiltersLex getLexer(String filter) {
        ANTLRInputStream input = new ANTLRInputStream(filter);
        FiltersLex lexer = new FiltersLex(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(ExceptionErrorListener.INSTANCE);
        return lexer;
    }
    public static FiltersParser getParser(FiltersLex lexer) {
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        FiltersParser parser = new FiltersParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(ExceptionErrorListener.INSTANCE);
        return parser;
    }
}
