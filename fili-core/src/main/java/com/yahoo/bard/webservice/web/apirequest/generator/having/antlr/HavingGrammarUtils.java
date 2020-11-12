// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.having.antlr;

import com.yahoo.bard.webservice.web.apirequest.generator.having.ExceptionErrorListener;
import com.yahoo.bard.webservice.web.havingparser.HavingsLex;
import com.yahoo.bard.webservice.web.havingparser.HavingsParser;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;

public class HavingGrammarUtils {

    public static HavingsLex getLexer(String havingQuery) {
        ANTLRInputStream input = new ANTLRInputStream(havingQuery);
        HavingsLex lexer = new HavingsLex(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(ExceptionErrorListener.INSTANCE);
        return lexer;
    }
    public static HavingsParser getParser(HavingsLex lexer) {
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        HavingsParser parser = new HavingsParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(ExceptionErrorListener.INSTANCE);
        return parser;
    }
}
