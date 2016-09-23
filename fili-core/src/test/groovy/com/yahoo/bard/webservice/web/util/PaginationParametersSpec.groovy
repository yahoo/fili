// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.util

import com.yahoo.bard.webservice.web.BadPaginationException

import spock.lang.Specification
import spock.lang.Unroll


class PaginationParametersSpec extends Specification {

    private static final String BIG = Long.toString(Long.MAX_VALUE)

    @Unroll
    def "build the correct handler when 'perPage' is #perPage and 'page' is #page"() {
        when: "We build the paginationParameters handler"
        PaginationParameters handler = new PaginationParameters(perPage, page)

        then: "The handler's perPage and page are as expected"
        handler.getPerPage() == expectedPerPage
        handler.getPage() == expectedPage

        where:
        perPage | page || expectedPerPage   || expectedPage
        "1"     | "2"  || 1                 || 2
        "2"     | "1"  || 2                 || 1
    }

    @Unroll
    def "When '#page' or '#perPage' is not a positive int, an exception is thrown"() {
        when: "We try to build paginationParameters with invalid values"
        new PaginationParameters(perPage, page)

        then: "The expected exception is thrown with the expected error message"
        BadPaginationException exception = thrown(BadPaginationException)
        exception.getMessage() == expectedError

        where:
        perPage | page  || expectedError
        "-1"  | "-1"    || errorMessage("perPage", "-1")
        "0"   | "-1"    || errorMessage("perPage", "0")
        "1"   | "-1"    || errorMessage("page", "-1")
        "-1"  | "0"     || errorMessage("perPage", "-1")
        "-1"  | "1"     || errorMessage("perPage", "-1")
        "1a"  | "1a"    || errorMessage("perPage", "1a")
        "1a"  | "1"     || errorMessage("perPage", "1a")
        "1"   | "1a"    || errorMessage("page", "1a")
        "1"   | "b"     || errorMessage("page", "b")
        "a"   | "1"     || errorMessage("perPage", "a")
        "a"   | "b"     || errorMessage("perPage", "a")
        "1.0" | "1"     || errorMessage("perPage", "1.0")
        "1"   | "1.0"   || errorMessage("page", "1.0")
        "1.0" | "1.0"   || errorMessage("perPage", "1.0")
        " 1"  | "1"     || errorMessage("perPage", " 1")
        "1 "  | "1"     || errorMessage("perPage", "1 ")
        " 1 " | "1"     || errorMessage("perPage", " 1 ")
        "1"   | " 1"    || errorMessage("page", " 1")
        "1"   | "1 "    || errorMessage("page", "1 ")
        "1"   | " 1 "   || errorMessage("page", " 1 ")
        " 1"  | "1 "    || errorMessage("perPage", " 1")
        BIG   | "1"     || errorMessage("perPage", BIG)
        "1"   | BIG     || errorMessage("page", BIG)
        BIG   | BIG     || errorMessage("perPage", BIG)
    }

    String errorMessage(String paramName, String paramValue) {
        "Parameter '$paramName' expected a positive integer but received: '$paramValue'"
    }

    @Unroll
    def "If perPage is #emptyPerPage or page is #emptyPage then we throw an error"() {
        when: "We try to build paginationParameters with at least one empty string as a parameter"
        new PaginationParameters(perPage, page)

        then: "The exception is thrown as expected"
        BadPaginationException exception = thrown(BadPaginationException)
        exception.getMessage() == expectedMessage

        where:
        perPage | page || expectedMessage
        ""      | "1"  || missingParamMessage("perPage")
        "1"     | ""   || missingParamMessage("page")
        ""      | ""   || missingParamMessage("perPage")

        emptyPerPage = perPage ?: "the empty string"
        emptyPage = page ?: "the empty string"
    }

    def "withPerPage returns a new PaginationParameter object with the updated perPage value" () {
        given:
        PaginationParameters originalParameters = new PaginationParameters(1, 3)

        expect:
        originalParameters.withPerPage("2") == new PaginationParameters(2, 3)
    }

    def "withPage returns a new PaginationParameter object with the updated page value" () {
        given:
        PaginationParameters originalParameters = new PaginationParameters(1, 3)

        expect:
        originalParameters.withPage("2") == new PaginationParameters(1, 2)
    }

    @Unroll
    def "Passing an invalid page #invalidPage to withPage generates an error" () {
        given:
        PaginationParameters originalParameters = new PaginationParameters(1, 3)

        when:
        originalParameters.withPage(invalidPage)

        then:
        thrown(BadPaginationException)

        where:
        invalidPage << ["0", "-1", "1.0", "2.3", "$Long.MAX_VALUE", "1a", "AndrewIsMostlyHarmless."]
    }

    @Unroll
    def "Passing an invalid perPage #invalidPerPage to withPerPage generates an error" () {
        given:
        PaginationParameters originalParameters = new PaginationParameters(1, 3)

        when:
        originalParameters.withPerPage(invalidPerPage)

        then:
        thrown(BadPaginationException)

        where:
        invalidPerPage << ["0", "-1", "1.0", "2.3", "$Long.MAX_VALUE", "1a", "AndrewIsMostlyHarmless."]
    }

    String missingParamMessage(String missingParam) {
        "Missing parameter '$missingParam.' Both 'perPage' and 'page' are required for pagination."
    }
}
