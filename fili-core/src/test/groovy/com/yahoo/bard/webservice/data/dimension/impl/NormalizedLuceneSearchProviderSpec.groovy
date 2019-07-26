// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionRow
import com.yahoo.bard.webservice.util.DimensionStoreKeyUtils
import com.yahoo.bard.webservice.web.util.PaginationParameters

import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.TextField
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.IndexReader
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.index.Term
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.ScoreDoc
import org.apache.lucene.search.TermQuery
import org.apache.lucene.search.TopDocs
import org.apache.lucene.store.Directory
import org.apache.lucene.store.MMapDirectory

import groovy.transform.NotYetImplemented
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.file.Files
import java.nio.file.Path

class NormalizedLuceneSearchProviderSpec extends Specification {

    class SimpleDimensionField implements DimensionField {

        final String name

        SimpleDimensionField(String name) {
            this.name = name
        }

        @Override
        String getName() {
            return name
        }

        @Override
        String getDescription() {
            return ""
        }
    }

    // The suffix "_column_key" is always appended to key fields
    final static String KEY_COLUMN_NAME = "key_column_key"
    final static String SEARCH_COLUMN_NAME = "__search"
    final static String DOC1_KEY = "1"
    final static String DOC2_KEY = "2"
    final static String DOC3_KEY = "3"
    final static String DOC4_KEY = "4"
    final static String DOC5_KEY = "5"
    final static String DOC6_KEY = "6"

    Path tempDirPath
    IndexSearcher searcher
    Directory memoryIndex

    Dimension dim
    Map<String, DimensionRow> dimSearchMapping
    DimensionRow dimRow_key1
    DimensionRow dimRow_key2
    DimensionRow dimRow_key3
    DimensionRow dimRow_key4
    DimensionRow dimRow_key5
    DimensionRow dimRow_key6

    NormalizedLuceneSearchProvider searchProvider

    // key to test: promo\u00e7\u00e3o
    def setup() {
        // first build a simple in memory test index
        tempDirPath = Files.createTempDirectory("lucene_test")
        tempDirPath.toFile().deleteOnExit()

        memoryIndex = new MMapDirectory(tempDirPath)
        IndexWriterConfig writerConfig = new IndexWriterConfig(new StandardAnalyzer())
        IndexWriter writer = new IndexWriter(memoryIndex, writerConfig)
        Document document = new Document()

        // Build the index to test against. Note that the real index is assumed to be properly constructed
        // none of the INDEXED text will be weird corner cases
        Field keyField = new TextField(KEY_COLUMN_NAME, "", Field.Store.YES)
        Field searchField = new TextField(SEARCH_COLUMN_NAME, "", Field.Store.YES)
        document.add(keyField)
        document.add(searchField)

        // add all of the data to the index
        keyField.setStringValue(DOC1_KEY)
        searchField.setStringValue("promocao")
        writer.updateDocument(new Term(KEY_COLUMN_NAME, DOC1_KEY), document)

        keyField.setStringValue(DOC2_KEY)
        searchField.setStringValue("garbage")
        writer.updateDocument(new Term(KEY_COLUMN_NAME, DOC2_KEY), document)

        keyField.setStringValue(DOC3_KEY)
        searchField.setStringValue("promocao but also with other stuff")
        writer.updateDocument(new Term(KEY_COLUMN_NAME, DOC3_KEY), document)

        keyField.setStringValue(DOC4_KEY)
        searchField.setStringValue("special\\+chars\\|\\|test\\-case")
        writer.updateDocument(new Term(KEY_COLUMN_NAME, DOC4_KEY), document)

        keyField.setStringValue(DOC5_KEY)
        searchField.setStringValue("123456")
        writer.updateDocument(new Term(KEY_COLUMN_NAME, DOC5_KEY), document)

        keyField.setStringValue(DOC6_KEY)
        searchField.setStringValue("promocao garbage")
        writer.updateDocument(new Term(KEY_COLUMN_NAME, DOC6_KEY), document)

        // close the index and writer
        writer.close()
        memoryIndex.close()


        // build a mock dimension to provide data to store hits for searching
        dim = Mock(Dimension)
        DimensionField keyDimField = new SimpleDimensionField("key")
        DimensionField otherDimField = new SimpleDimensionField("other")

        dim.getKey() >> keyDimField
        dim.getDimensionFields() >> {
            [
                    keyDimField,
                    otherDimField
            ] as LinkedHashSet<DimensionField>
        }

        dimSearchMapping = [:]
        dimRow_key1 = new DimensionRow(
                keyDimField,
                [
                        (keyDimField) : DOC1_KEY,
                        (otherDimField) : "other1"
                ]
        )
        dimSearchMapping.put(DOC1_KEY, dimRow_key1)

        dimRow_key2 = new DimensionRow(
                keyDimField,
                [
                        (keyDimField) : DOC2_KEY,
                        (otherDimField) : "other2"
                ]
        )
        dimSearchMapping.put(DOC2_KEY, dimRow_key2)

        dimRow_key3 = new DimensionRow(
                keyDimField,
                [
                        (keyDimField) : DOC3_KEY,
                        (otherDimField) : "other3"
                ]
        )
        dimSearchMapping.put(DOC3_KEY, dimRow_key3)

        dimRow_key4 = new DimensionRow(
                keyDimField,
                [
                        (keyDimField) : DOC4_KEY,
                        (otherDimField) : "other4"
                ]
        )
        dimSearchMapping.put(DOC4_KEY, dimRow_key4)

        dimRow_key5 = new DimensionRow(
                keyDimField,
                [
                        (keyDimField) : DOC5_KEY,
                        (otherDimField) : "other5"
                ]
        )
        dimSearchMapping.put(DOC5_KEY, dimRow_key5)

        dimRow_key6 = new DimensionRow(
                keyDimField,
                [
                        (keyDimField) : DOC6_KEY,
                        (otherDimField) : "other6"
                ]
        )
        dimSearchMapping.put(DOC6_KEY, dimRow_key6)

        searchProvider = new NormalizedLuceneSearchProvider(tempDirPath.toString(), 10000)
        searchProvider.setDimension(dim)

        dim.findDimensionRowByKeyValue(_) >> { String value -> dimSearchMapping.get(value)}
    }

    /* Setting up a lucene index to test against is finicky and a little difficult to understand. this test just
       creates a reader on the index set up and parses it to make sure everything is working correctly. Should be a
       sanity check for this Spec to make sure everything is working more or less as expected.
    */
    def "Sanity check that test index is created properly"() {
        // Now open a reader against the index
        setup:
        memoryIndex = new MMapDirectory(tempDirPath)
        IndexReader indexReader = DirectoryReader.open(memoryIndex)
        searcher = new IndexSearcher(indexReader)

        when:
        TopDocs topDocs = searcher.search(new TermQuery(new Term(SEARCH_COLUMN_NAME, "promocao")), 10)

        then:
        topDocs.scoreDocs.length == 3

        when:
        List<String> docs = []
        for (ScoreDoc doc: topDocs.scoreDocs) {
            docs.addAll(searcher.doc(doc.doc).getValues(KEY_COLUMN_NAME))
        }

        then:
        [DOC1_KEY, DOC3_KEY, DOC6_KEY].each {it -> assert docs.contains(it)}

        cleanup:
        indexReader.close()
        memoryIndex.close()
    }

    @Unroll
    def "test validating column #indexedOrUnindexed #columnName #expected"() {
        expect:
        expected == searchProvider.validateSearchColumn(columnName)

        where:
        columnName          | expected  | indexedOrUnindexed
        KEY_COLUMN_NAME     | true      | "indexed"
        SEARCH_COLUMN_NAME  | true      | "indexed"
        "notindexed"        | false     | "not indexed"

    }

    def "test simple case for lucene search provider"() {
        when:
        List<DimensionRow> result = searchProvider.findSearchRowsPaged("promocao", PaginationParameters.EVERYTHING_IN_ONE_PAGE).getPageOfData()
        List<String> resultKeys = result.collect { it -> it.getKeyValue() }

        then:
        result.size() == 3
        resultKeys.size() == 3

        and:
        [DOC1_KEY, DOC3_KEY, DOC6_KEY] .each { it -> assert resultKeys.contains(it) }
    }

    def "test words with latin accents are properly searched on (i.e. converted to plain ascii equivalent and used to search on)"() {
        when:
        List<DimensionRow> result = searchProvider.findSearchRowsPaged("promo\u00e7\u00e3o", PaginationParameters.EVERYTHING_IN_ONE_PAGE).getPageOfData()
        List<String> resultKeys = result.collect { it -> it.getKeyValue() }

        then:
        result.size() == 3
        resultKeys.size() == 3

        and:
        [DOC1_KEY, DOC3_KEY, DOC6_KEY] .each { it -> assert resultKeys.contains(it) }
    }

    def "test query string with reserved characters successfully finds string indexed with escaped reserved characters"() {
        when:
        List<DimensionRow> result = searchProvider.findSearchRowsPaged("special+chars||test-case", PaginationParameters.EVERYTHING_IN_ONE_PAGE).getPageOfData()
        List<String> resultKeys = result.collect { it -> it.getKeyValue() }

        then:
        result.size() == 1
        resultKeys.size() == 1

        and:
        [DOC4_KEY].each { it -> assert resultKeys.contains(it) }
    }

    def "test searching on two terms treats them as an AND instead of an OR"() {
        when:
        List<DimensionRow> result = searchProvider.findSearchRowsPaged("promocao garbage", PaginationParameters.EVERYTHING_IN_ONE_PAGE).getPageOfData()
        List<String> resultKeys = result.collect { it -> it.getKeyValue() }

        then:
        result.size() == 1
        resultKeys.size() == 1

        and:
        [DOC6_KEY].each { it -> assert resultKeys.contains(it) }
    }

    def "test searching on nonindexed text does not return any results"() {
        when:
        List<DimensionRow> result = searchProvider.findSearchRowsPaged("fail", PaginationParameters.EVERYTHING_IN_ONE_PAGE).getPageOfData()

        then:
        result.size() == 0
    }

    @Unroll
    def "generating search query doesn't fail on #desc"() {
        when:
        searchProvider.getSearchQuery(weirdQuery)

        then:
        noExceptionThrown()

        where:
        weirdQuery  |   desc
        "   hello"  |   "leading whitespace"
        "hello   "  |   "trailing whitespace"
        "hello\n"   |   "trailing newline"
        "b\b"       |   "1 char followed by backspace"
        "1"         |   "query is just a number"
        "@@@@@@"    |   "non reserved special characters"
        "& |"       |   "part of reserved set of chars"
        "AND"       |   "Lucene query syntax"
        ""                   | "empty string"
        "\u0020\u0020\u0020" | "whitespace"
        "\t\t\t"             | "tabs"
        "\n \r"              | "newline and carriage return"
        "\b\b\b"             | "backspaces"
        "\u0001\u0001"       | "ctrl-A"
        Character.toChars(128579).toString()    |   "emoji"
        "+ - && || ! ( ) { } [ ] ^ \" ~ * ? : \\ /"    |   "reserved characters"
    }

    def "setting a new dimension on the lucene search provider overwrites the existing analyzer wrapper with a new one for that dimension"() {
        setup:
        Dimension dim1 = Mock() {
            getDimensionFields() >> {
                [
                        Mock(DimensionField) { getName() >> "dim1_field1" },
                        Mock(DimensionField) { getName() >> "dim1_field2" }
                ] as LinkedHashSet<DimensionField>
            }
        }
        Dimension dim2 = Mock() {
            getDimensionFields() >> {
                [
                        Mock(DimensionField) { getName() >> "dim2_field1" },
                        Mock(DimensionField) { getName() >> "dim2_field2" }
                ] as LinkedHashSet<DimensionField>
            }
        }

        searchProvider = new NormalizedLuceneSearchProvider(tempDirPath.toString(), 10000)

        when:
        searchProvider.setDimension(dim1)
        PerFieldAnalyzerWrapper analyzer = (PerFieldAnalyzerWrapper) searchProvider.analyzer

        then:
        analyzer.fieldAnalyzers.size() == 3
        analyzer.fieldAnalyzers.containsKey(DimensionStoreKeyUtils.getColumnKey("dim1_field1"))
        analyzer.fieldAnalyzers.containsKey(DimensionStoreKeyUtils.getColumnKey("dim1_field2"))
        analyzer.fieldAnalyzers.containsKey(NormalizedLuceneSearchProvider.SEARCH_COLUMN_NAME)

        when:
        searchProvider.setDimension(dim2)
        analyzer = (PerFieldAnalyzerWrapper) searchProvider.analyzer

        then:
        analyzer.fieldAnalyzers.size() == 3
        analyzer.fieldAnalyzers.containsKey(DimensionStoreKeyUtils.getColumnKey("dim2_field1"))
        analyzer.fieldAnalyzers.containsKey(DimensionStoreKeyUtils.getColumnKey("dim2_field2"))
        analyzer.fieldAnalyzers.containsKey(NormalizedLuceneSearchProvider.SEARCH_COLUMN_NAME)
    }
}
