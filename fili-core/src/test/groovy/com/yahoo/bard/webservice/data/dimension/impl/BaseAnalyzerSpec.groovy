// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute

import spock.lang.Specification

/**
 * Base class for testing custom analyzers. Contains convenience methods useful in these tests.
 */
abstract class BaseAnalyzerSpec extends Specification {
    //convenience method for extracting parsed tokens from provided string using provided analyzer
    def getTokensFromText(Analyzer analyzer, String field, String text) {
        List<String> result = []
        TokenStream tokenStream = analyzer.tokenStream(field, new StringReader(text))
        CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class)
        tokenStream.reset()

        while(tokenStream.incrementToken()) {
            result.add(charTermAttribute.toString())
        }
        tokenStream.close()
        return result
    }
}
