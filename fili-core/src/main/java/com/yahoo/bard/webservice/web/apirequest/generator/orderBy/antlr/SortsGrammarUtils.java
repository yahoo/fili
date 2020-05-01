// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.orderBy.antlr;

import com.yahoo.bard.webservice.web.apirequest.generator.having.ExceptionErrorListener;
import com.yahoo.bard.webservice.web.sorts.SortsLex;
import com.yahoo.bard.webservice.web.sorts.SortsParser;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;

public class SortsGrammarUtils {

    public static SortsLex getLexer(String sortQuery) {
        ANTLRInputStream input = new ANTLRInputStream(sortQuery);
        SortsLex lexer = new SortsLex(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(ExceptionErrorListener.INSTANCE);
        return lexer;
    }
    public static SortsParser getParser(SortsLex lexer) {
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SortsParser parser = new SortsParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(ExceptionErrorListener.INSTANCE);
        return parser;
    }
}
