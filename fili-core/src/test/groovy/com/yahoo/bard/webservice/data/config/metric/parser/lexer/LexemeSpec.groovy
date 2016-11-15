// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.lexer

import spock.lang.Specification

public class LexemeSpec extends Specification {
    def "lexeme should know its length"() {
        setup:
        def lexeme = new Lexeme(LexemeType.IDENTIFIER, "foo")

        expect:
        lexeme.getConsumedLength() == "foo".length()
    }

    def "lexing a simple input should work"() {
        setup:
        def lexemes = Lexeme.lex("3 + 4").collect()

        expect:
        lexemes == [
                new Lexeme(LexemeType.NUMBER, "3"),
                new Lexeme(LexemeType.BINARY_OPERATOR, "+"),
                new Lexeme(LexemeType.NUMBER, "4"),
        ]
    }

    def "lexing an invalid input should fail"() {
        when:
        Lexeme.lex("3 ? 4")

        then:
        LexException ex = thrown()
        ex.message =~ /Viable option for lexing could not be found.*/
    }

    def "lexing a more complex input should work"() {
        setup:
        def lexemes = Lexeme.lex("3 * 4 + (9/5) - impressions + foo(bar, bat) | a == 'one' && c == \"two\"").collect()
        expect:
        lexemes == [
                new Lexeme(LexemeType.NUMBER, "3"),
                new Lexeme(LexemeType.BINARY_OPERATOR, "*"),
                new Lexeme(LexemeType.NUMBER, "4"),
                new Lexeme(LexemeType.BINARY_OPERATOR, "+"),
                new Lexeme(LexemeType.L_PAREN, "("),
                new Lexeme(LexemeType.NUMBER, "9"),
                new Lexeme(LexemeType.BINARY_OPERATOR, "/"),
                new Lexeme(LexemeType.NUMBER, "5"),
                new Lexeme(LexemeType.R_PAREN, ")"),
                new Lexeme(LexemeType.BINARY_OPERATOR, "-"),
                new Lexeme(LexemeType.IDENTIFIER, "impressions"),
                new Lexeme(LexemeType.BINARY_OPERATOR, "+"),
                new Lexeme(LexemeType.IDENTIFIER, "foo"),
                new Lexeme(LexemeType.L_PAREN, "("),
                new Lexeme(LexemeType.IDENTIFIER, "bar"),
                new Lexeme(LexemeType.COMMA, ","),
                new Lexeme(LexemeType.IDENTIFIER, "bat"),
                new Lexeme(LexemeType.R_PAREN, ")"),
                new Lexeme(LexemeType.PIPE, "|"),
                new Lexeme(LexemeType.IDENTIFIER, "a"),
                new Lexeme(LexemeType.FILTER_OPERATOR, "=="),
                new Lexeme(LexemeType.SINGLE_QUOTED_STRING, "one", 5),
                new Lexeme(LexemeType.FILTER_OPERATOR, "&&"),
                new Lexeme(LexemeType.IDENTIFIER, "c"),
                new Lexeme(LexemeType.FILTER_OPERATOR, "=="),
                new Lexeme(LexemeType.DOUBLE_QUOTED_STRING, "two", 5),
        ]
    }
}
