// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import spock.lang.Specification
import spock.lang.Unroll

class EitherSpec extends Specification {

    @Unroll
    def "A #leftRight Either<String, Integer> wrapping '#value' can be unwrapped into '#value'"() {
        expect: "We can unwrap an Either into the value it wraps"
        unwrap(wrap(value)) == value

        where:
        leftRight | wrap               | unwrap     || value
        "left"    | {Either.left(it)}  | {it.left}  || "string"
        "right"   | {Either.right(it)} | {it.right} || 5
    }

    @Unroll
    def "A #leftRight Either<String, Integer> #isIsNot left"() {
        expect:
        wrap(value).isLeft() == expectedLeftOrNot

        where:
        leftRight | wrap               | isIsNot  | value    || expectedLeftOrNot
        "left"    | {Either.left(it)}  | "is"     | "string" || true
        "right"   | {Either.right(it)} | "is not" | 5        || false
    }

    @Unroll
    def "A #leftRight Either<String, Integer> #isOrIsNot right"() {
        expect:
        wrap(value).isRight() == expectedRightOrNot

        where:
        leftRight | wrap               | isOrIsNot | value    || expectedRightOrNot
        "right"   | {Either.right(it)} | "is"      | "string" || true
        "left"    | {Either.left(it)}  | "is not"  | 5        || false
    }

    @Unroll
    def "Attempting to get the #rightLeftValue from a #leftRight Either throws UnsupportedOperationException"() {
        when:
        invalidUnwrap(wrap(value))

        then:
        thrown UnsupportedOperationException

        where:
        rightLeftValue | leftRight | wrap               | invalidUnwrap || value
        "right"        | "left"    | {Either.left(it)}  | {it.right}    || "string"
        "left"         | "right"   | {Either.right(it)} | {it.left}     || 5
    }
}