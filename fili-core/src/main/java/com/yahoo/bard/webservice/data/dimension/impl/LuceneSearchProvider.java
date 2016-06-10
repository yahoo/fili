// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import com.yahoo.bard.webservice.data.cache.HashDataCache.Pair;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.util.DimensionStoreKeyUtils;
import com.yahoo.bard.webservice.util.Utils;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.RowLimitReachedException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * LuceneSearchProvider
 * Search provider which uses lucene
 */
public class LuceneSearchProvider implements SearchProvider {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneSearchProvider.class);

    private static final Analyzer LUCENE_ANALYZER = new StandardAnalyzer();
    private static final double BUFFER_SIZE = 48;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final String luceneIndexPath;
    private final int hitsPerPage;
    private int maxResultsWithoutFilters;

    private Directory luceneDirectory;
    private KeyValueStore keyValueStore;
    private Dimension dimension;
    private boolean luceneIndexIsHealthy;
    private IndexSearcher luceneIndexSearcher;

    public LuceneSearchProvider(String luceneIndexPath, int hitsPerPage, int maxResultsWithoutFilters) {
        this.luceneIndexPath = luceneIndexPath;
        Utils.createParentDirectories(this.luceneIndexPath);

        this.hitsPerPage = hitsPerPage;
        this.maxResultsWithoutFilters = maxResultsWithoutFilters;

        try {
            luceneDirectory = new MMapDirectory(Paths.get(this.luceneIndexPath));
            luceneIndexIsHealthy = true;
        } catch (IOException e) {
            luceneIndexIsHealthy = false;
            String message = String.format("Unable to create index directory %s:", this.luceneIndexPath);
            LOG.error(message, e);
        }
    }

    private IndexSearcher getIndexSearcher() {
        // Open the searcher if we don't have one
        if (luceneIndexSearcher == null) {
            reopenIndexSearcher(true);
        }
        return luceneIndexSearcher;
    }

    private synchronized void reopenIndexSearcher(boolean firstTimeThrough) {
        try {
            // Close the current reader if open
            if (luceneIndexSearcher != null) {
                luceneIndexSearcher.getIndexReader().close();
            }

            // Open a new IndexSearcher on a new DirectoryReader
            luceneIndexSearcher = new IndexSearcher(DirectoryReader.open(luceneDirectory));

        } catch (IOException reopenException) {
            // If there is no index file, this is expected. On the 1st time through, write an empty index and try again
            if (firstTimeThrough) {
                IndexWriterConfig indexWriterConfig = new IndexWriterConfig(LUCENE_ANALYZER);
                lock.writeLock().lock();
                try (IndexWriter luceneIndexWriter = new IndexWriter(luceneDirectory, indexWriterConfig)) {
                    //Closed automatically by the try-resource block
                } catch (IOException emptyIndexWriteException) {
                    // We can't move past this, so puke
                    luceneIndexIsHealthy = false;
                    String message = String.format("Unable to write empty index to %s:", luceneIndexPath);
                    LOG.error(message, emptyIndexWriteException);
                    throw new RuntimeException(emptyIndexWriteException);
                } finally {
                    lock.writeLock().unlock();
                }

                reopenIndexSearcher(false);
            } else {
                // We've been here before, so puke
                luceneIndexIsHealthy = false;
                String message = String.format("Unable to open index searcher for %s:", luceneIndexPath);
                LOG.error(message, reopenException);
                throw new RuntimeException(reopenException);
            }
        }
    }

    protected void setMaxResultsWithoutFilters(int maxResultsWithoutFilters) {
        this.maxResultsWithoutFilters = maxResultsWithoutFilters;
    }

    @Override
    public void setDimension(Dimension dimension) {
        this.dimension = dimension;
    }

    @Override
    public void setKeyValueStore(KeyValueStore keyValueStore) {
        this.keyValueStore = keyValueStore;
        // Check initialization for the cardinality in a keyValueStore
        if (keyValueStore.get(DimensionStoreKeyUtils.getCardinalityKey()) == null) {
            keyValueStore.put(DimensionStoreKeyUtils.getCardinalityKey(), "0");
        }
    }

    @Override
    public int getDimensionCardinality() {
        return Integer.parseInt(keyValueStore.get(DimensionStoreKeyUtils.getCardinalityKey()));
    }

    @Override
    public Set<DimensionRow> findAllDimensionRows() {
        int numRows = getDimensionCardinality();
        if (numRows <= maxResultsWithoutFilters) {

            TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(hitsPerPage);
            IndexSearcher indexSearcher = getIndexSearcher();

            try {
                indexSearcher.search(new MatchAllDocsQuery(), topScoreDocCollector);
            } catch (IOException e) {
                LOG.error("Unable to find all dimension rows");
                throw new RuntimeException(e);
            }

            String idKey = DimensionStoreKeyUtils.getColumnKey(dimension.getKey().getName());
            return Arrays.stream(topScoreDocCollector.topDocs().scoreDocs)
                    .map(
                            hit -> {
                                try {
                                    return indexSearcher.doc(hit.doc);
                                } catch (IOException e) {
                                    LOG.error("Unable to find all dimension rows");
                                    throw new RuntimeException(e);
                                }
                            }
                    )
                    .map(document -> document.get(idKey))
                    .map(dimension::findDimensionRowByKeyValue)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        } else {
            String msg = String.format(
                    "Cardinality = %d exceeds maximum number of rows = %d allowed without filters",
                    numRows,
                    maxResultsWithoutFilters
            );
            throw new RowLimitReachedException(msg);
            //TODO: Not supported in phase 1 - listing all dimension values for large dimension
        }
    }

    @Override
    public TreeSet<DimensionRow> findAllOrderedDimensionRows() {
        return new TreeSet<>(findAllDimensionRows());
    }

    @Deprecated
    @Override
    public Set<DimensionRow> findAllDimensionRowsByField(DimensionField dimField, String fieldValue) {
        /**
         * Here idKey is for fetching the id column value from lucene index
         * as value for only key column is stored in lucene indexes i.e Field.Store.YES
         * and fieldKey is for performing search on indexes
         *
         * eg:
         * For a dimension dim1 with key field as ID and other field as DESC
         * idKey will be id_column_key
         * fieldKey will be desc_column_key
         */

        String fieldKey = DimensionStoreKeyUtils.getColumnKey(dimField.getName());
        TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(hitsPerPage);
        IndexSearcher indexSearcher = getIndexSearcher();

        try {
            /**
             * TODO: There is scope for optimization here by using TermQuery instead of WildcardQuery,
             * For some reason I couldn't get the TermQuery to work here, it always returns empty response
             */
            Query q = new WildcardQuery(new Term(fieldKey, "*" + fieldValue + "*"));
            indexSearcher.search(q, topScoreDocCollector);
        } catch (IOException e) {
            LOG.error("Unable to find dimension rows by field {} with value {}", dimField, fieldValue);
            throw new RuntimeException(e);
        }

        String idKey = DimensionStoreKeyUtils.getColumnKey(dimension.getKey().getName());
        return Arrays.stream(topScoreDocCollector.topDocs().scoreDocs)
                .map(
                        hit -> {
                            try {
                                return indexSearcher.doc(hit.doc);
                            } catch (IOException e) {
                                LOG.error(
                                        "Unable to find dimension rows by field {} with value {}",
                                        dimField,
                                        fieldValue
                                );
                                throw new RuntimeException(e);
                            }
                        }
                )
                .map(document -> document.get(idKey))
                .map(dimension::findDimensionRowByKeyValue)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Override
    public boolean isHealthy() {
        return luceneIndexIsHealthy;
    }

    /**
     * Refresh lucene index
     *
     * Query the lucene indexes on key column to see if there are any existing indexes.
     *
     * If yes, then we need to update the indexes.
     * Update is done by first deleting the existing documents and then adding new ones.
     *
     * If not, then just add the documents to indexes.
     *
     *
     * @param rowId  key for id column
     * @param dimensionRow  the new dimension row
     * @param dimensionRowOld  the dimension row with which the index needs to be added / updated
     */
    @Override
    public void refreshIndex(String rowId, DimensionRow dimensionRow, DimensionRow dimensionRowOld) {
        refreshIndex(Collections.singletonMap(rowId, new Pair<>(dimensionRow, dimensionRowOld)));
    }

    @Override
    public void refreshIndex(Map<String, Pair<DimensionRow, DimensionRow>> changedRows) {
        // Make a single Document instance to hold field data being updated to Lucene
        // Creating documents is costly and so Document will be reused for each record being processed due to
        // performance best practices.
        Document doc = new Document();
        Map<DimensionField, Field> dimFieldToLuceneField = new HashMap<>(dimension.getDimensionFields().size());

        // Create the document fields for this dimension and add them to the document
        for (DimensionField dimensionField : dimension.getDimensionFields()) {
            Field luceneField = new StringField(
                    DimensionStoreKeyUtils.getColumnKey(dimensionField.getName()),
                    "",
                    dimensionField.equals(dimension.getKey()) ? Field.Store.YES : Field.Store.NO
            );

            // Store the lucene field in the doc and in our lookup map
            dimFieldToLuceneField.put(dimensionField, luceneField);
            doc.add(luceneField);
        }

        // Write the rows to the document
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(LUCENE_ANALYZER).setRAMBufferSizeMB(BUFFER_SIZE);
        lock.writeLock().lock();
        try (IndexWriter luceneIndexWriter = new IndexWriter(luceneDirectory, indexWriterConfig)) {
            // Update the document fields for each row and update the document
            for (String rowId : changedRows.keySet()) {
                // Get the new row from the pair
                DimensionRow newDimensionRow = changedRows.get(rowId).getKey();

                // Update the index
                updateDimensionRow(doc, dimFieldToLuceneField, luceneIndexWriter, newDimensionRow);
            }

            // Commit all the changes to the index (on .close, called by try-resources) and refresh the cardinality
        } catch (IOException e) {
            luceneIndexIsHealthy = false;
            LOG.error("Failed to refresh index for dimension rows", e);
            throw new RuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
        reopenIndexSearcher(true);
        refreshCardinality();
    }

    /**
     * Update the dimension row in the index.
     *
     * @param luceneDimensionRowDoc  Document to use for doing the update
     * @param fieldMap  Mapping of DimensionFields to the Document's fields
     * @param writer  Lucene IndexWriter to update the indexes of
     * @param newRow  Row to update
     *
     * @throws IOException if there is a problem updating the document
     */
    private void updateDimensionRow(
            Document luceneDimensionRowDoc,
            Map<DimensionField, Field> fieldMap,
            IndexWriter writer,
            DimensionRow newRow
    ) throws IOException {
        // Update the document fields with each field from the new dimension row
        for (DimensionField field : dimension.getDimensionFields()) {
            // Get the field to update from the lookup map
            Field fieldToUpdate = fieldMap.get(field);

            // Set field value to updated value
            fieldToUpdate.setStringValue(newRow.get(field));
        }

        // Build the term to delete the old document by the key value (which should be unique)
        Term keyTerm = new Term(fieldMap.get(dimension.getKey()).name(), newRow.get(dimension.getKey()));

        // Update the document by the key term
        writer.updateDocument(keyTerm, luceneDimensionRowDoc);
    }

    @Override
    public void clearDimension() {
        Set<DimensionRow> dimensionRows = findAllDimensionRows();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(LUCENE_ANALYZER).setRAMBufferSizeMB(BUFFER_SIZE);
        lock.writeLock().lock();
        try (IndexWriter writer = new IndexWriter(luceneDirectory, indexWriterConfig)) {
            //Remove all dimension data from the store.
            String rowId = dimension.getKey().getName();
            dimensionRows.stream()
                    .map(DimensionRow::getRowMap)
                    .map(map -> map.get(rowId))
                    .map(id -> DimensionStoreKeyUtils.getRowKey(rowId, id))
                    .forEach(keyValueStore::remove);

            //Since Lucene's indices are being dropped, the dimension field stored via the columnKey is becoming stale.
            keyValueStore.remove(DimensionStoreKeyUtils.getColumnKey(dimension.getKey().getName()));
            //The allValues key mapping needs to reflect the fact that we are dropping all dimension data.
            keyValueStore.put(DimensionStoreKeyUtils.getAllValuesKey(), "[]");
            //We're resetting the keyValueStore, so we don't want any stale last updated date floating around.
            keyValueStore.remove(DimensionStoreKeyUtils.getLastUpdatedKey());

            //In addition to clearing the keyValueStore, we also need to delete all of Lucene's segment files.
            writer.deleteAll();
            writer.commit();
            reopenIndexSearcher(true);
            refreshCardinality();
        } catch (IOException e) {
            LOG.error("Failed to wipe Lucene index at directory: {}", luceneDirectory);
            throw new RuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Update the cardinality count.
     */
    private void refreshCardinality() {
        keyValueStore.put(
                DimensionStoreKeyUtils.getCardinalityKey(),
                Integer.toString(getIndexSearcher().getIndexReader().numDocs())
        );
    }

    /**
     * Fetch filtered set of rows.
     * An empty set of filters evaluates to "true" (i.e. every dimension row will be returned).
     *
     * @param filters  The set of filters
     * @return The set of dimension rows
     */
    @Override
    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    public TreeSet<DimensionRow> findFilteredDimensionRows(Set<ApiFilter> filters) {
        /*
        Intuitively, Lucene performs searching for each BooleanQuery as follows:
        1. Start with an empty set of results.
        2. Find all the results that MUST and/or SHOULD appear
        3. Remove those entries that MUST_NOT appear

        Therefore, if a query has nothing but MUST_NOTs, then we end up removing elements from the
        original empty set of results. This is true even if the query appears in a nested query.

        For example, consider the filter:
        property|id-in[sports,finance],location|id-notin[US,Canada],language|id-in[english], which simplifies by
        DeMorgan's Law into

        (sports || finance) && (!US && !Canada) && (english)

        If we naively translated each clause in parenthesis into their own boolean queries, and then chained them
        together using MUST, we would always get an empty set of results, because (!US && !Canada) and is an
        individual query with nothing but MUST_NOT.

        Therefore, we need to guarantee that our negative clauses are not treated as queries. Fortunately,
        by the associativity of conjunction, the above query is equivalent to:

        (sports || finance) && !US && !Canada && english or in Lucene Speak:

        MUST(SHOULD(sports), SHOULD(finance)), MUST_NOT(US), MUST_NOT(Canada), MUST(english)
         */
        BooleanQuery.Builder filterQueryBuilder = new BooleanQuery.Builder();
        boolean hasPositive = false;
        for (ApiFilter filter : filters) {
            String luceneFieldName = DimensionStoreKeyUtils.getColumnKey(filter.getDimensionField().getName());
            switch (filter.getOperation()) {
                case eq:
                    // fall through on purpose since eq and in have the same functionality
                case in:
                    hasPositive = true;
                    filterQueryBuilder.add(inFilterQuery(luceneFieldName, filter), BooleanClause.Occur.MUST);
                    break;
                case notin:
                    //Add each negative clause to the top-level query
                    filterToTermQueries(luceneFieldName, filter)
                            .forEach(query -> filterQueryBuilder.add(query, BooleanClause.Occur.MUST_NOT));
                    break;
                case startswith:
                    filterQueryBuilder.add(startswithFilterQuery(luceneFieldName, filter), BooleanClause.Occur.MUST);
                    hasPositive = true;
                    break;
                case contains:
                    filterQueryBuilder.add(containsFilterQuery(luceneFieldName, filter), BooleanClause.Occur.MUST);
                    hasPositive = true;
                    break;
                default:
                    LOG.debug("Illegal Filter operation : {}", filter.getOperation());
                    throw new IllegalArgumentException("Invalid Filter Operation.");
            }
        }

        if (!hasPositive) {
            //If we don't have any positive queries, then we want the entire Universe, except for values that match
            //the negative queries.
            filterQueryBuilder.add(new BooleanClause(new MatchAllDocsQuery(), BooleanClause.Occur.MUST));
        }

        TopScoreDocCollector topScoreDocCollector = TopScoreDocCollector.create(hitsPerPage);
        IndexSearcher indexSearcher = getIndexSearcher();

        //Search for documents that match the final query
        try {
            indexSearcher.search(filterQueryBuilder.build(), topScoreDocCollector);
        } catch (IOException e) {
            LOG.error("Unable to fetch filtered rows");
            throw new RuntimeException(e);
        }

        String idKey = DimensionStoreKeyUtils.getColumnKey(dimension.getKey().getName());
        return Arrays.stream(topScoreDocCollector.topDocs().scoreDocs)
                .map(
                        hit -> {
                            try {
                                return indexSearcher.doc(hit.doc);
                            } catch (IOException e) {
                                LOG.error("Unable to fetch filtered rows");
                                throw new RuntimeException(e);
                            }
                        }
                )
                .map(document -> document.get(idKey))
                .map(dimension::findDimensionRowByKeyValue)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * Returns a Collector that accumulates boolean queries into a single nested query, and ties all of the sub
     * queries together with the specified {@link BooleanClause.Occur}
     *
     * @param occur  The Occur operator to tie the boolean queries together with
     *
     * @return A Collector that ties a collection of boolean queries into a single query with the specified
     * occurrence operator
     */
    private static Collector<Query, BooleanQuery.Builder, BooleanQuery.Builder> getBooleanQueryCollector(
            BooleanClause.Occur occur
    ) {
        return Collector.of(
                BooleanQuery.Builder::new,
                (builder, termQuery) -> builder.add(termQuery, occur),
                (accumulator, termQueryBuilder) -> accumulator.add(termQueryBuilder.build(), occur)
        );
    }

    /**
     * In-filter operation
     *
     * @param luceneFieldName  Name of the lucene field to filter on
     * @param filter  New filter to add to the query
     *
     * @return A builder that knows how to build the appropriate BooleanQuery
     */
    private BooleanQuery inFilterQuery(String luceneFieldName, ApiFilter filter) {
        return filterToTermQueries(luceneFieldName, filter)
                .collect(getBooleanQueryCollector(BooleanClause.Occur.SHOULD))
                .build();
    }

    /**
     * Given an ApiFilter, returns a stream of term queries, one for each value in the filter.
     *
     * @param luceneFieldName  Name of the lucene field to filter on
     * @param filter  The filter to be turned into term queries
     *
     * @return A stream of term queries
     */
    private Stream<TermQuery> filterToTermQueries(String luceneFieldName, ApiFilter filter) {
        return filter.getValues().stream()
                .map(value -> new Term(luceneFieldName, value))
                .map(TermQuery::new);
    }

    /**
     * Startswith-filter operation
     *
     * @param luceneFieldName  Name of the lucene field to filter on
     * @param filter  New filter to add to the query
     *
     * @return A builder that knows how to build the appropriate BooleanQuery
     */
    private BooleanQuery startswithFilterQuery(String luceneFieldName, ApiFilter filter) {
        return filter.getValues().stream()
                .map(value -> new Term(luceneFieldName, value))
                .map(PrefixQuery::new)
                .collect(getBooleanQueryCollector(BooleanClause.Occur.SHOULD))
                .build();
    }

    /**
     * Contains filter operation
     *
     * @param luceneFieldName  Name of the lucene field to filter on
     * @param filter  New filter to add to the query
     *
     * @return A builder that knows how to build the appropriate BooleanQuery
     */
    private BooleanQuery containsFilterQuery(String luceneFieldName, ApiFilter filter) {
        return filter.getValues().stream()
                .map(value -> new Term(luceneFieldName, "*" + value + "*"))
                .map(WildcardQuery::new)
                .collect(getBooleanQueryCollector(BooleanClause.Occur.SHOULD))
                .build();
    }
}
