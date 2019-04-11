// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionRow
import com.yahoo.bard.webservice.data.dimension.KeyValueStore
import com.yahoo.bard.webservice.data.dimension.TimeoutException
import com.yahoo.bard.webservice.util.DimensionStoreKeyUtils
import com.yahoo.bard.webservice.web.ErrorMessageFormat
import com.yahoo.bard.webservice.web.RowLimitReachedException
import com.yahoo.bard.webservice.web.util.PaginationParameters

import org.apache.commons.io.FileUtils
import org.apache.lucene.store.FSDirectory

import spock.lang.Ignore
import spock.lang.Timeout

import java.nio.file.Files
import java.nio.file.Path

/**
 * Specification for behavior specific to the LuceneSearchProvider
 */
class LuceneSearchProviderSpec extends SearchProviderSpec<LuceneSearchProvider> {

    int rowLimit
    int searchTimeout

    String sourceDir
    String destinationDir

    Path sourcePath
    Path file1
    Path file2
    Path file3
    Path subDir
    Path file4
    Path destinationPath

    @Override
    void childSetup() {
        //Clears compiler warnings from IntelliJ. Can't use the getter, because that requires knowledge of the
        //dimension name, which this Spec does not have access to.
        rowLimit = searchProvider.maxResults
        searchTimeout = searchProvider.searchTimeout

        sourceDir = "target/tmp/dimensionCache/animal/new_lucene_indexes"
        destinationDir = "target/tmp/dimensionCache/animal/lucene_indexes"

        sourcePath = Files.createDirectory(new File(sourceDir).getAbsoluteFile().toPath())
        destinationPath = new File(destinationDir).getAbsoluteFile().toPath()

        file1 = sourcePath.resolve("segments_1")
        file2 = sourcePath.resolve("_1.cfs")
        file3 = sourcePath.resolve("_1.si")
        subDir = sourcePath.resolve("subDir")

        Files.createFile(file1)
        Files.createFile(file2)
        Files.createFile(file3)
        Files.createDirectory(subDir)

        file4 = subDir.resolve("subDirFile")
        Files.createFile(file4)
    }

    @Override
    void childCleanup() {
        searchProvider.maxResults = rowLimit
        searchProvider.searchTimeout = searchTimeout

        FileUtils.deleteDirectory(new File(sourceDir))
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

    def "refresh cardinality is called when assigining a new key value store"() {
        given: "a new key value store"
        KeyValueStore keyValueStore = Mock()

        when: "lucene gets a new key value store with no cardinality key"
        searchProvider.setKeyValueStore(keyValueStore)

        then: "the cardinality is set to the current lucene document count"
        1 * keyValueStore.put('cardinality_key', '14')
    }

    def "refresh cardinality is called when getting cardinality with refresh"() {
        given: "a new key value store"
        KeyValueStore keyValueStore = Mock()
        keyValueStore.getOrDefault(_, "0") >> "12"

        when: "lucene gets a new key value store with no cardinality key"
        searchProvider.setKeyValueStore(keyValueStore)
        searchProvider.getDimensionCardinality(true) == 12

        then: "the cardinality is set to the current lucene document count"
        2 * keyValueStore.put('cardinality_key', '14')
    }

    def "moveDirEntries moves all entries of a directory to a new directory, while keeping all old empty dirs"() {
        expect:
        Files.exists(sourcePath)
        Files.exists(file1)
        Files.exists(file2)
        Files.exists(file3)
        Files.exists(subDir)
        Files.exists(file4)

        !Files.exists(destinationPath.resolve("segments_1"))
        !Files.exists(destinationPath.resolve("_1.cfs"))
        !Files.exists(destinationPath.resolve("_1.si"))
        !Files.exists(destinationPath.resolve("subDir"))
        !Files.exists(destinationPath.resolve("subDir").resolve("subDirFile"))

        when:
        searchProvider.moveDirEntries(sourceDir, destinationDir)

        then:
        Files.exists(sourcePath)
        !Files.exists(file1)
        !Files.exists(file2)
        !Files.exists(file3)
        Files.exists(subDir)
        !Files.exists(file4)

        and:
        Files.exists(destinationPath.resolve("segments_1"))
        Files.exists(destinationPath.resolve("_1.cfs"))
        Files.exists(destinationPath.resolve("_1.si"))
        Files.exists(destinationPath.resolve("subDir"))
        Files.exists(destinationPath.resolve("subDir").resolve("subDirFile"))
    }

    def "deleteDir deletes all entries under a specified directory including the directory itself"() {
        expect:
        Files.exists(sourcePath)
        Files.exists(file1)
        Files.exists(file2)
        Files.exists(file3)
        Files.exists(subDir)
        Files.exists(file4)

        when:
        searchProvider.deleteDir(sourceDir)

        then:
        !Files.exists(sourcePath)
        !Files.exists(file1)
        !Files.exists(file2)
        !Files.exists(file3)
        !Files.exists(subDir)
        !Files.exists(file4)
    }

    @Timeout(5)
    def "If time waiting for write lock exceeds timeout fail query"() {
        setup:
        searchProvider.@searchTimeout = 2000

        when:
        // thread that just gets read lock and holds it
        Thread t = new Thread({searchProvider.readLock()})
        t.start()
        t.join()
        searchProvider.writeLock()

        then:
        Exception e = thrown(IllegalStateException)
        e.getMessage() == String.format(
                ErrorMessageFormat.LUCENE_LOCK_TIMEOUT.getMessageFormat(),
                searchProvider.getDimension().getApiName()
        )
    }

    @Timeout(5)
    def "If time waiting for read lock exceeds timeout fail query"() {
        setup:
        searchProvider.@searchTimeout = 2000

        when:
        // thread that just gets write lock and holds it
        Thread t = new Thread({searchProvider.writeLock()})
        t.start()
        t.join()
        searchProvider.readLock()

        then:
        Exception e = thrown(IllegalStateException)
        e.getMessage() == String.format(
                ErrorMessageFormat.LUCENE_LOCK_TIMEOUT.getMessageFormat(),
                searchProvider.getDimension().getApiName()
        )
    }

    @Ignore("This test is currently not valid because the replacement index is invalid.")
    def "replaceIndex hot-swaps Lucene indexes in place"() {
        given:
        // destination = "target/tmp/dimensionCache/animal/lucene_indexes", where we will keep indexes all the time
        Path oldIndexFile = destinationPath.resolve("segments_2")
        Files.createFile(oldIndexFile)

        expect:
        Files.exists(destinationPath)
        Files.exists(oldIndexFile)

        when:
        // source = "target/tmp/dimensionCache/animal/new_lucene_indexes", where new indexes come from
        searchProvider.replaceIndex(sourceDir)

        then:
        Files.exists(destinationPath)
        !Files.exists(oldIndexFile)

        and:
        Files.exists(destinationPath.resolve("segments_1"))
        Files.exists(destinationPath.resolve("_1.cfs"))
        Files.exists(destinationPath.resolve("_1.si"))
        Files.exists(destinationPath.resolve("subDir"))
        Files.exists(destinationPath.resolve("subDir").resolve("subDirFile"))
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
        void run() {
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
        }
    }

    def "MultiThread refreshIndex"() {

        when:
        List<TestThread> threads = [ new TestThread(), new TestThread(), new TestThread() ]

        then:
        threads.each { it.start() }
        threads.each { it.join(10000) }
        threads.each {
            if (it.cause != null) {
                throw it.cause
            } }
    }
}
