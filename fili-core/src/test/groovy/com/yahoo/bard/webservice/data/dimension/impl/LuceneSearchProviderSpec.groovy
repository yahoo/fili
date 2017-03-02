// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionRow
import com.yahoo.bard.webservice.data.dimension.TimeoutException
import com.yahoo.bard.webservice.util.DimensionStoreKeyUtils
import com.yahoo.bard.webservice.web.RowLimitReachedException
import com.yahoo.bard.webservice.web.util.PaginationParameters
import org.apache.lucene.store.FSDirectory
/**
 * Specification for behavior specific to the LuceneSearchProvider
 */
class LuceneSearchProviderSpec extends SearchProviderSpec<LuceneSearchProvider> {

    int rowLimit;
    int searchTimeout;

    @Override
    void childSetup() {
        //Clears compiler warnings from IntelliJ. Can't use the getter, because that requires knowledge of the
        //dimension name, which this Spec does not have access to.
        rowLimit = searchProvider.maxResults
        searchTimeout = searchProvider.searchTimeout
    }

    @Override
    void childCleanup() {
        searchProvider.maxResults = rowLimit
        searchProvider.searchTimeout = searchTimeout
    }

    @Override
    LuceneSearchProvider getSearchProvider(String dimensionName) {
        return LuceneSearchProviderManager.getInstance(dimensionName)
    }

    @Override
    void cleanSearchProvider(String dimensionName) {
        LuceneSearchProviderManager.removeInstance(dimensionName)
    }

    def "findAllDimensionRows throws exception when the lucene search timeout is reached"() {
        given:
        searchProvider.searchTimeout = -1

        when:
        searchProvider.findAllDimensionRowsPaged(new PaginationParameters(rowLimit, 1))

        then:
        thrown TimeoutException
    }

    def "findAllDimensionRows doesn't throw an exception when the real cardinality is less than the limit"() {
        given:
        searchProvider.maxResults = PaginationParameters.EVERYTHING_IN_ONE_PAGE.getPerPage()

        expect:
        searchProvider.findAllDimensionRows() == dimensionRows as Set
    }

    def "findAllDimensionRows throws RowLimitReachedException exception when the number of results per page is more than the limit"() {
        when:
        searchProvider.findAllDimensionRowsPaged(new PaginationParameters(rowLimit+1, 1))

        then:
        thrown RowLimitReachedException
    }

    def "findAllOrderedDimensionRows doesn't throw an exception when the number of results per page is less than the limit "() {
        given:
        searchProvider.maxResults = PaginationParameters.EVERYTHING_IN_ONE_PAGE.getPerPage()

        expect:
        searchProvider.findAllOrderedDimensionRows() == dimensionRows as Set
    }

    def "findAllOrderedDimensionRows throws RowLimitReachedException exception when the real cardinality is more than the limit"() {
        given: "A row limit below the total number of rows"
        searchProvider.maxResults = dimensionRows.size() - 1

        when: "We try to get all the rows"
        searchProvider.findAllOrderedDimensionRows()

        then: "An exception is thrown because too many rows are returned"
        thrown RowLimitReachedException
    }

    @Override
    boolean indicesHaveBeenCleared() {
        //A file is a Lucene index file iff it has one of the following extensions
        List<String> luceneIndexExtensions = ["cfs", "cfe", "fnm", "fdx", "fdt", "tis", "tii", "frq", "prx", "nrm",
                                              "tv", "tvd", "tvf", "del"]

        //There aren't any Lucene index files.
        boolean filesPresent = (searchProvider.luceneDirectory as FSDirectory).getDirectory().toFile().listFiles()
                .find {luceneIndexExtensions.contains(it.getName().tokenize('.')[-1]) }

        //We assume that this test is being run with a MapStore backing the LuceneSearchProvider.
        !filesPresent &&
                searchProvider.keyValueStore.store.size() == 2 &&
                searchProvider.keyValueStore[DimensionStoreKeyUtils.getCardinalityKey()] == "0" &&
                searchProvider.keyValueStore[DimensionStoreKeyUtils.getAllValuesKey()] == "[]"
    }

    class TestThread extends Thread {
        Throwable cause = null

        @Override
        public void run() {
            try {
                DimensionRow dimensionRow3new = BardDimensionField.makeDimensionRow(
                        keyValueStoreDimension,
                        "kumquat",
                        "this is still not an animal"
                )
                DimensionRow dimensionRow4 = BardDimensionField.makeDimensionRow(
                        keyValueStoreDimension,
                        "badger",
                        "Badger badger badger badger mushroom mushroom badger badger badger"
                )
                keyValueStoreDimension.addDimensionRow(dimensionRow3new)
                Thread.sleep(100)
                keyValueStoreDimension.addDimensionRow(dimensionRow4)
            } catch (Throwable t) {
                cause = t
            }
        }
    }

    def "MultiThread refreshIndex"() {

        when:
        List<TestThread> threads = [ new TestThread(), new TestThread(), new TestThread() ]

        then:
        threads.each { it.start() }
        threads.each { it.join(10000) }
        threads.each { if ( it.cause != null ) throw it.cause }
    }
}
