// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.lexer;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;

/**
 * A lexeme represents a single input token and contains the type of token and its content.
 *
 * This class also contains utility methods for producing lexemes for an entire string.
 */
public class Lexeme {

    protected final String token;
    protected final LexemeType type;
    protected final int length;

    /**
     * Construct a new lexeme.
     *
     * @param type the type of lexeme
     * @param token the content
     */
    public Lexeme(LexemeType type, String token) {
        this(type, token, token.length());
    }

    /**
     * Construct a new lexeme.
     *
     * @param type the type of lexeme
     * @param token the content
     * @param length the consumed length of the token
     */
    public Lexeme(LexemeType type, String token, int length) {
        this.type = type;
        this.token = token;
        this.length = length;
    }

    /**
     * Get the lexeme content.
     *
     * @return the content of this lexeme
     */
    public String getToken() {
        return token;
    }

    /**
     * Get the lexeme type.
     *
     * @return the LexemeType
     */
    public LexemeType getType() {
        return type;
    }

    /**
     * Get the consumed lexeme length.
     *
     * Typically equal to the size of the token, but in special cases may be longer.
     * For example, 'foo' has a token of foo (no quotes) but length of 5.
     *
     * @return integer length
     */
    public int getConsumedLength() {
        return length;
    }

    /**
     * Return Lexemes parsed from the input string.
     *
     * @param input the input string to be lexed
     * @return A queue of tokens/lexemes
     * @throws LexException when an error occurs
     */
    public static Queue<Lexeme> lex(String input) throws LexException {
        Queue<Lexeme> lexemes = new LinkedList<>();
        int pos = 0;

        // Read the entire input.
        // We break out of loop when:
        // - we get to the end of the line (get it?)
        // - we reach a position at which there is no valid lexeme (exception thrown)
        while (pos < input.length()) {
            // All whitespace between lexemes is skipped
            while (pos < input.length() && (input.charAt(pos) == '\t' || input.charAt(pos) == ' ')) {
                pos++;
            }
            if (pos >= input.length()) {
                break;
            }

            // Throws a LexException if no possible lexeme can be found
            Lexeme lexeme = getNextLexeme(input.substring(pos));
            lexemes.add(lexeme);
            pos += lexeme.getConsumedLength();
        }

        return lexemes;
    }

    /**
     * Get the next lexeme.
     *
     * @param inputString the input string
     * @return The lexeme
     * @throws LexException if a viable lexeme can not be created
     */
    protected static Lexeme getNextLexeme(String inputString) throws LexException {
        return Arrays.stream(LexemeType.values())
                .map(a -> a.buildLexeme(inputString))
                .filter(Objects::nonNull)
                .findFirst()
                .orElseThrow(
                        () -> new LexException("Viable option for lexing could not be found", inputString));
    }


    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof Lexeme)) {
            return false;
        }

        Lexeme other = (Lexeme) obj;
        return Objects.equals(token, other.token) &&
                Objects.equals(type, other.type) &&
                Objects.equals(length, other.length);
    }

    @Override
    public int hashCode() {
        return Objects.hash(token, type, length);
    }

    @Override
    public String toString() {
        return "Lexeme<type=" + type + " token=" + token + " length=" + length + ">";
    }
}
