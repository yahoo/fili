// Copyright 2019, Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.util.DimensionStoreKeyUtils;
import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.web.util.PaginationParameters;
import com.yahoo.bard.webservice.data.dimension.SearchQuerySearchProvider;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.simple.SimpleQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Lucene search provider that also supports text search queries.
 *
 * If the search indexes have been indexed with a field named "__search", this search provider
 * will prepare and run queries against the search column, using a standard parser enhanced with
 * a Diacritic normalizer (e.g. turning accented letters into unaccented english equivalents).
 *
 * The expected contract is that interesting fields from the main indexes will be tokenized,
 * normalized and indexed on the '__search', so as to allow look ahead search use cases in clients.
 *
 */
public class NormalizedLuceneSearchProvider extends LuceneSearchProvider implements SearchQuerySearchProvider {
    private static final Logger LOG = LoggerFactory.getLogger(NormalizedLuceneSearchProvider.class);

    /**
    * Name of the lucene index column that the search endpoint is using.
    */
    public static final String SEARCH_COLUMN_NAME = "__search";
    public static final Analyzer DIACRITIC_ANALYZER = new DiacriticNormalizingAnalyzer();

    private SimpleQueryParser queryParser;

    private boolean searchColumnExists;
    private IndexSearcher lastIndexSearcher;

    /**
     * Constructor.
     *
     * @param luceneIndexPath  Path to the lucene index files
     * @param maxResults  Maximum number of allowed results in a page
     * @param searchTimeout  Maximum time in milliseconds that a lucene search can run
     */
    public NormalizedLuceneSearchProvider(String luceneIndexPath, int maxResults, int searchTimeout) {
        super(luceneIndexPath, maxResults, searchTimeout);

        // override analyzer in LuceneSearchProvider
        Map<String, Analyzer> analyzerMap = new HashMap<>();
        analyzerMap.put(SEARCH_COLUMN_NAME, DIACRITIC_ANALYZER);

        setAnalyzer(new PerFieldAnalyzerWrapper(STANDARD_LUCENE_ANALYZER, analyzerMap));

        this.queryParser = new SimpleQueryParser(analyzer, SEARCH_COLUMN_NAME);
        this.queryParser.setDefaultOperator(BooleanClause.Occur.MUST);
    }

    /**
     * Constructor.  The search timeout is initialized to the default (or configured) value.
     *
     * @param luceneIndexPath  Path to the lucene index files
     * @param maxResults  Maximum number of allowed results in a page
     */
    public NormalizedLuceneSearchProvider(String luceneIndexPath, int maxResults) {
        this(luceneIndexPath, maxResults, LUCENE_SEARCH_TIMEOUT_MS);
    }

    @Override
    public Pagination<DimensionRow> findSearchRowsPaged(
            String searchQueryString,
            PaginationParameters paginationParameters
    ) {
       initializeIndexSearcher();
       readLock();
       try {
           validateSearchColumn();

           return getResultsPage(getSearchQuery(searchQueryString), paginationParameters);
       } finally {
           readUnlock();
       }
    }

    /**
     * If the search provider has changed, recheck that the search column is available and error if not.
     */
    private void validateSearchColumn() {
         if (lastIndexSearcher != luceneIndexSearcher) {
             lastIndexSearcher = luceneIndexSearcher;
             searchColumnExists = validateSearchColumn(SEARCH_COLUMN_NAME);
         }
        if (!searchColumnExists) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Underlying LuceneIndex for Dimension %s does not support search queries.",
                            getDimension()
                    )
            );
        }
    }

    protected Analyzer getAnalyzer() {
        return analyzer;
    }

    protected void setAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    @Override
    public void setDimension(Dimension dimension) {
        super.setDimension(dimension);

        setAnalyzer(buildNewAnalyzerMapFromDimension(getDimension()));
        queryParser = new SimpleQueryParser(analyzer, SEARCH_COLUMN_NAME);
        queryParser.setDefaultOperator(BooleanClause.Occur.MUST);
    }

    /**
     * Builds a new analyzer map using the provided dimension. Maps all fields of the provided dimension to use the
     * standard lucene analyzer and sets the search column to use the diacritic unaware analyzer.
     *
     * @param dimension Dimension to build the new analyzer map off of
     * @return the new analyzer
     */
    protected Analyzer buildNewAnalyzerMapFromDimension(Dimension dimension) {
        Map<String, Analyzer> analyzerMap = dimension.getDimensionFields().stream()
                .map(field -> DimensionStoreKeyUtils.getColumnKey(field.getName()))
                .collect(Collectors.toMap(
                        Function.identity(),
                        unused -> STANDARD_LUCENE_ANALYZER
                ));
        analyzerMap.put(SEARCH_COLUMN_NAME, DIACRITIC_ANALYZER);

        return new PerFieldAnalyzerWrapper(STANDARD_LUCENE_ANALYZER, analyzerMap);
    }

    /**
     * Generate the Lucene search query from the provided search query string. The search query string is intended to
     * be provided by a user.
     *
     * @param searchQueryString  the query string to convert into a Lucene Query object
     * @return the query object
     */
    private Query getSearchQuery(String searchQueryString) {
        return queryParser.parse(QueryParser.escape(searchQueryString));
    }

    /**
     * Validates that the search column with the provided name exists, by checking that more than 0 documents have a
     * field with the provided name.
     *
     * @param searchColumnName The name of the search column to check.
     * @return whether or not that field exists in the lucene index.
     */
    protected boolean validateSearchColumn(String searchColumnName) {
        initializeIndexSearcher();
        readLock();
        try {
            return luceneIndexSearcher.getIndexReader().getDocCount(searchColumnName) > 0;
        } catch (IOException e) {
            LOG.debug(
                    String.format(
                            "Failed to open IndexReader for LuceneIndex on Dimension %s. Failed with error:\n%s",
                            getDimension(),
                            e.getMessage()
                    )
            );
            // No need to directly fail the query on this. Just return that search is not supported on the index
            return false;
        } finally {
            readUnlock();
        }
    }
}
