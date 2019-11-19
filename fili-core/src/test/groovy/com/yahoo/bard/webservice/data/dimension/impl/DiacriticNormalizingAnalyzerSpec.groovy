// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.LowerCaseFilter
import org.apache.lucene.analysis.StopFilter
import org.apache.lucene.analysis.StopwordAnalyzerBase
import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.analysis.Tokenizer
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter
import org.apache.lucene.analysis.ngram.NGramTokenFilter
import org.apache.lucene.analysis.standard.StandardTokenizer
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.TextField
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.IndexReader
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.queryparser.simple.SimpleQueryParser
import org.apache.lucene.search.BooleanClause
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.PhraseQuery
import org.apache.lucene.search.Query
import org.apache.lucene.search.TopDocs
import org.apache.lucene.store.Directory
import org.apache.lucene.store.MMapDirectory
import org.apache.lucene.store.RAMDirectory

import spock.lang.Unroll

import java.nio.file.Files
import java.nio.file.Path

/**
 * Tests diacritic normalizing analyzer properly replaces non-ascii latin characters with their latin "equivalents".
 */
class DiacriticNormalizingAnalyzerSpec extends BaseAnalyzerSpec {

    @Unroll
    def "test diacritic analyzer #desc"() {
        setup:
        Analyzer analyzer = new DiacriticNormalizingAnalyzer()
        List<String> tokens = getTokensFromText(analyzer, "test", text)

        expect:
        tokens == expected_tokens

        where:
        text                           |   expected_tokens     |   desc
        "H\u00e9LL\u00f4 W\u00f2rlD"   |   ["hello", "world"]  |   "properly converts latin diacritics"
        "H\u00e9LL\u00f4 \u6f22"       |   ["hello", "\u6f22"] |   "does not affect non-latin non-ascii characters"
    }

    def "test words with diacritics before analysis are properly prioritized after analysis"() {
        setup:
        // setup writing
        String text1 = "H\u00e9LL\u00f4 W\u00f2rlD"
        String text2 = "garbagegarbagegarbage hello world garbagegarbagegarbage"
        Path testPath = Files.createTempDirectory("test_diacritic_normalization")
        testPath.toFile().deleteOnExit()
        Directory directory = new MMapDirectory(testPath)

        Analyzer analyzer = new DiacriticNormalizingAnalyzer()
        IndexWriter writer = new IndexWriter(
                directory,
                new IndexWriterConfig(analyzer)
        )

        Document doc1 = new Document()
        doc1.add(new TextField(
                "testField",
                text1,
                Field.Store.YES
        ))
        writer.addDocument(doc1)
        Document doc2 = new Document()
        doc2.add(new TextField(
                "testField",
                text2,
                Field.Store.YES
        ))
        writer.addDocument(doc2)
        writer.close()

        and:
        // setup reading
        IndexReader reader = DirectoryReader.open(directory)
        IndexSearcher searcher = new IndexSearcher(reader)

        when:
        Query query = new PhraseQuery("testField", "hello", "world")
        TopDocs hits = searcher.search(query, 1)

        then:
        hits.totalHits.intValue() == 2

        when:
        Document hitDoc = searcher.doc(hits.scoreDocs[0].doc)

        then:
        hitDoc.get("testField") == "H\u00e9LL\u00f4 W\u00f2rlD"

        cleanup:
        reader.close()
    }

    @Unroll
    def "test using diacritic normalizing analyzer against an ngram index will properly find results, even on partial word queries"() {
        setup:
        // setup writing
        String text = "hello world"
        Path testPath = Files.createTempDirectory("test_diacritic_normalization")
        testPath.toFile().deleteOnExit()
        Directory directory = new MMapDirectory(testPath)

        IndexWriter writer = new IndexWriter(
                directory,
                new IndexWriterConfig(ngramAnalyzer)
        )

        Document doc = new Document()
        doc.add(new TextField(
                "testField",
                text,
                Field.Store.YES
        ))
        writer.addDocument(doc)
        writer.close()

        and:
        // setup reading
        IndexReader reader = DirectoryReader.open(directory)
        IndexSearcher searcher = new IndexSearcher(reader)

        when:
        SimpleQueryParser sqp = new SimpleQueryParser(new DiacriticNormalizingAnalyzer(), "testField")
        sqp.setDefaultOperator(BooleanClause.Occur.MUST)
        Query query = sqp.parse(queryText)
        TopDocs hits = searcher.search(query, 1)

        then:
        hits.totalHits.intValue() == 1

        when:
        Document hitDoc = searcher.doc(hits.scoreDocs[0].doc)

        then:
        hitDoc.get("testField") == "hello world"

        cleanup:
        reader.close()

        where:
        queryText << ["hello world", "hel", "orl"]
    }

    def  getNgramAnalyzer() {
        new StopwordAnalyzerBase() {
            @Override
            protected Analyzer.TokenStreamComponents createComponents(String fieldName) {
                Tokenizer source = new StandardTokenizer()

                TokenStream tokenStream = source
                tokenStream = new LowerCaseFilter(tokenStream)
                tokenStream = new StopFilter(tokenStream, getStopwordSet())
                tokenStream = new NGramTokenFilter(tokenStream, 3, 5, true)
                return new Analyzer.TokenStreamComponents(source, tokenStream)
            }
        }
    }
}
