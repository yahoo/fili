// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.misc.ParseCancellationException;

/**
 * Basic ANTLR error listener that throws ParseCancellationExceptions.
 */
public class ExceptionErrorListener extends BaseErrorListener {

    public static final ExceptionErrorListener INSTANCE = new ExceptionErrorListener();

    @Override
    public void syntaxError(
            Recognizer<?, ?> recognizer,
            Object offendingSymbol,
            int line,
            int charPos,
            String msg,
            RecognitionException e
    ) throws ParseCancellationException {
        throw new ParseCancellationException("line " + line + ":" + charPos + " " + msg, e);
    }
}
