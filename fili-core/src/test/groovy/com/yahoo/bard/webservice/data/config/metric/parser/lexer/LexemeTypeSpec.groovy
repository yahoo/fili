// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.lexer

import spock.lang.Specification
import spock.lang.Unroll

public class LexemeTypeSpec extends Specification {

    @Unroll
    def "LexemeTypes should build correct lexemes"(String lexString, LexemeType type) {
        setup:
        def lexeme = type.buildLexeme(lexString)
        expect:
        lexeme.equals(new Lexeme(type, lexString))
        lexeme.getConsumedLength() == lexString.length()

        where:
        lexString | type
        "+"       | LexemeType.BINARY_OPERATOR
        "-"       | LexemeType.BINARY_OPERATOR
        "*"       | LexemeType.BINARY_OPERATOR
        "/"       | LexemeType.BINARY_OPERATOR

        "&&"      | LexemeType.FILTER_OPERATOR
        "||"      | LexemeType.FILTER_OPERATOR
        "=="      | LexemeType.FILTER_OPERATOR

        "|"       | LexemeType.PIPE

        ","       | LexemeType.COMMA
        "("       | LexemeType.L_PAREN
        ")"       | LexemeType.R_PAREN

        "3"       | LexemeType.NUMBER
        "3.14"    | LexemeType.NUMBER
        ".14"     | LexemeType.NUMBER
        "0.14"    | LexemeType.NUMBER

        "FOO"     | LexemeType.IDENTIFIER
        "Foo"     | LexemeType.IDENTIFIER
    }

    def "Double quoted strings should include quotes in length but not in content"() {
        setup:
        def lexeme = LexemeType.DOUBLE_QUOTED_STRING.buildLexeme('"foo"')
        expect:

        lexeme.getConsumedLength() == 5
        lexeme.getType() == LexemeType.DOUBLE_QUOTED_STRING
        lexeme.getToken() == "foo"
    }

    def "Single quoted strings should include quotes in length but not in content"() {
        setup:
        def lexeme = LexemeType.SINGLE_QUOTED_STRING.buildLexeme("'foo'")
        expect:

        lexeme.getConsumedLength() == 5
        lexeme.getType() == LexemeType.SINGLE_QUOTED_STRING
        lexeme.getToken() == "foo"
    }

    def "Null should be returned for non-matching string"() {
        setup:
        def result = LexemeType.BINARY_OPERATOR.buildLexeme("not a binary operator")

        expect:
        result == null
    }

    def "Null should be returned for non-matching pattern"() {
        setup:
        def result = LexemeType.IDENTIFIER.buildLexeme("+ is not a valid identifier")

        expect:
        result == null
    }

    def "Pattern should build lexeme with only matching part of pattern"() {
        setup:
        def result = LexemeType.IDENTIFIER.buildLexeme("foo + bar")

        expect:
        result.equals(new Lexeme(LexemeType.IDENTIFIER, "foo"))
    }
}
