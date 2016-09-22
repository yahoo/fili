// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import com.yahoo.bard.webservice.web.PageNotFoundException
import com.yahoo.bard.webservice.web.util.PaginationParameters

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.core.Response


class PaginationSpec extends Specification {

    @Shared
    Pagination pagination1;
    @Shared
    Pagination pagination2;
    @Shared
    Pagination pagination3;

    @Shared
    static final LinkedHashSet<Integer> CONTENT = new LinkedHashSet<>((1..27))

    static final int ROWS_PER_PAGE = 5
    static final int NUM_PAGES = 6

    def setupSpec(){
        //first page
        pagination1 = new AllPagesPagination(CONTENT, new PaginationParameters(ROWS_PER_PAGE, 1))
        // second page
        pagination2 = new AllPagesPagination(CONTENT, new PaginationParameters(ROWS_PER_PAGE, 2))
        // last page
        pagination3 = new AllPagesPagination(CONTENT, new PaginationParameters(ROWS_PER_PAGE, NUM_PAGES))
    }

    def "test getPaginatedResult()"() {
        expect:
        [6, 7, 8, 9, 10] == pagination2.getPageOfData()
        [26, 27] == pagination3.getPageOfData()
    }

    def "test getLastPage()"() {
        expect:
        pagination2.getLastPage().getAsInt() == 6
    }

    def "test valid getNextPage()"() {
        expect:
        pagination2.getNextPage().getAsInt() == 3
    }

    def "test invalid getNextPage()"() {
        expect:
        pagination3.getNextPage() == OptionalInt.empty()
    }

    def "test valid getPreviousPage()"() {
        expect:
        pagination2.getPreviousPage().getAsInt() == 1
    }

    def "test invalid getPreviousPage()"() {
        expect:
        pagination1.getPreviousPage() == OptionalInt.empty()
    }

    @Unroll
    def "An exception is thrown when page requested is #pagePastTheLast and number of pages is #numPages"(){
        when: "We build a pagination object that tries to fetch a page past the last"
        new AllPagesPagination<Integer>(CONTENT, new PaginationParameters(ROWS_PER_PAGE, pagePastTheLast))

        then: "We get a PageNotFoundException with the expected error message."
        PageNotFoundException exception = thrown(PageNotFoundException)
        exception.getMessage() == getExpectedErrorMessage(pagePastTheLast, ROWS_PER_PAGE, numPages)
        exception.getErrorStatus() == Response.Status.NOT_FOUND

        where:
        pagePastTheLast << [NUM_PAGES + 1, NUM_PAGES + 2, Integer.MAX_VALUE]
        numPages = NUM_PAGES
    }

    def "When the result set is empty, and we request the first page, we get an empty result set"() {
        given: "A pagination object with an empty result set and a desired first page."
        Pagination<Integer> pagination = new AllPagesPagination<>(
                Collections.emptySet(),
                new PaginationParameters(ROWS_PER_PAGE, 1)
        )

        expect: "The desired page is empty"
        pagination.getPageOfData().isEmpty()
    }

    def "When the result set is empty, and we request a page other than the first, we get a PageNotFoundError" () {
        when: "We built a pagination object with an empty result set and a desired page other than the first"
        new AllPagesPagination<Integer>(Collections.emptySet(), new PaginationParameters(ROWS_PER_PAGE, 2))

        then: "We throw an error"
        thrown(PageNotFoundException)
    }

    @Unroll
    def "A page has #remainingRows rows of results rather than than #rowsPerPage rows if and only if it last." () {
        given: "A collection of results whose size is not divided evenly by rowsPerPage"
        int numResults = rowsPerPage + remainingRows
        List<Integer> results = (1..numResults)

        when: "We built the pagination object"
        Pagination<Integer> pagination = new AllPagesPagination<>(
                results,
                new PaginationParameters(rowsPerPage, numResults / rowsPerPage + 1 as Integer)
        )

        then: "The page requested is the last page."
        pagination.getNextPage() == OptionalInt.empty()

        and: "The page has the rows"
        pagination.getPageOfData() == (rowsPerPage + 1 .. numResults).collect()

        where:
        rowsPerPage | remainingRows
        2           | 1
        3           | 2
        6           | 5
    }

    @Unroll
    def "When #rowsPerPage are requested with only #rows rows of data, there is only one page, with all the results"() {
        given: "A data set"
        List<Integer> results = (1..rows).collect()

        when: "We build the pagination object"
        Pagination<Integer> pagination = new AllPagesPagination<>(results, new PaginationParameters(rowsPerPage, 1))

        then: "There is only one page"
        pagination.getNextPage() == OptionalInt.empty()
        pagination.getPreviousPage() == OptionalInt.empty()

        and: "The page has all the results"
        pagination.getPageOfData() == results

        where:
        rowsPerPage | rows
        2           | 1
        10          | 7
    }

    String getExpectedErrorMessage(Integer page, Integer rowsPerPage, Integer numPages) {
        "Requested page '$page' with '$rowsPerPage' rows per page, but there are only '$numPages' pages."
    }
}
