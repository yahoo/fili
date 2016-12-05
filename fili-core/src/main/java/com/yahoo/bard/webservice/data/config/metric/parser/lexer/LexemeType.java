// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.lexer;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Available token types.
 *
 * The order these are defined is the order that matching will be attempted.
 */
public enum LexemeType {

    /**
     * A binary arithmetic operator: +, -, *, /.
     */
    BINARY_OPERATOR("+", "-", "*", "/"),

    /**
     * A Druid filter operator: &amp;&amp;, ||, ==.
     *
     * Must be above PIPE since OR looks like two pipes.
     */
    FILTER_OPERATOR("&&", "||", "=="),

    /**
     * A pipe (filter-by).
     */
    PIPE("|"),

    /**
     * A "double quoted string" that can have slash-escaped double quotes in it.
     */
    DOUBLE_QUOTED_STRING(Pattern.compile("^\"((?:[^\"\\\\]*(?:\\\\.)?)*)\"")),

    /**
     * A 'single quoted string' that can have slash-escaped quotes in it.
     */
    SINGLE_QUOTED_STRING(Pattern.compile("^'((?:[^'\\\\]*(?:\\\\.)?)*)'")),

    /**
     * A comma.
     */
    COMMA(","),

    /**
     * A left paren.
     */
    L_PAREN("("),

    /**
     * A right paren.
     */
    R_PAREN(")"),

    /**
     * Any number (not including sign).
     *
     * Note that '100.' is a false positive here - it matches '100', but lexing will fail when you reach '.' as that
     * character is invalid
     */
    NUMBER(Pattern.compile("^(([0-9]\\d*\\.\\d+)|(\\.\\d+)|([1-9][0-9]*))")),

    /**
     * Any string identifier.
     */
    IDENTIFIER(Pattern.compile("^([a-zA-Z_][a-zA-Z0-9_]*)"));

    // An enum constant's LexemeBuilder can try to create a lexeme of its own type
    protected final LexemeBuilder builder;

    /**
     * Lexeme type matched by a set of valid strings.
     *
     * @param validStrings valid strings
     */
    LexemeType(String... validStrings) {
        this.builder = (inputString) -> Arrays.stream(validStrings)
                .filter(inputString::startsWith)
                .map(s -> new Lexeme(this, s))
                .findFirst()
                .orElse(null);
    }

    /**
     * Lexeme type matched by a regex.
     *
     * The regex should match the content it wants in group 1; the full length of the match
     * will be used as the lexeme length.
     *
     * For example, for a single quoted string like 'foo', the consumed length is 5
     * but the returned token is [foo] (unquoted).
     *
     * This removes a little complexity in the parser; it doesn't have to deal
     * with quotes at all. It also means in the lexer we can discard insignificant
     * whitespace, etc.
     *
     * @param pattern valid pattern
     */
    LexemeType(Pattern pattern) {
        this.builder = (inputString) -> {
            Matcher matcher = pattern.matcher(inputString);
            if (matcher.find()) {
                return new Lexeme(this, matcher.group(1), matcher.group(0).length());
            }
            return null;
        };
    }

    /**
     * Attempt to build a lexeme for the current LexemeType.
     *
     * Will return null if no match can be constructed.
     *
     * @param inputString the current input
     * @return a Lexeme of the current type, or null if no Lexeme for the current type can be built
     */
    public Lexeme buildLexeme(String inputString) {
        return builder.build(inputString);
    }
}
