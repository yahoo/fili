// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.cache.HashDataCache.Pair;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.TimeoutException;
import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;
import com.yahoo.bard.webservice.util.DimensionStoreKeyUtils;
import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.util.SinglePagePagination;
import com.yahoo.bard.webservice.util.Utils;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;
import com.yahoo.bard.webservice.web.PageNotFoundException;
import com.yahoo.bard.webservice.web.RowLimitReachedException;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import org.apache.commons.io.FileUtils;
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
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TimeLimitingCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * LuceneSearchProvider.
 * Search provider which uses lucene.
 */
public class LuceneSearchProvider implements SearchProvider {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneSearchProvider.class);

    private static final Analyzer LUCENE_ANALYZER = new StandardAnalyzer();
    private static final double BUFFER_SIZE = 48;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final String luceneIndexPath;

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    public static final int LUCENE_SEARCH_TIMEOUT_MS = SYSTEM_CONFIG.getIntProperty(
            SYSTEM_CONFIG.getPackageVariableName("lucene_search_timeout_ms"), 600000
    );

    /**
     * The maximum number of results per page.
     */
    private int maxResults;

    private Directory luceneDirectory;
    private KeyValueStore keyValueStore;
    private Dimension dimension;
    private boolean luceneIndexIsHealthy;
    private IndexSearcher luceneIndexSearcher;
    private int searchTimeout;

    /**
     * Constructor.
     *
     * @param luceneIndexPath  Path to the lucene index files
     * @param maxResults  Maximum number of allowed results in a page
     * @param searchTimeout  Maximum time in milliseconds that a lucene search can run
     */
    public LuceneSearchProvider(String luceneIndexPath, int maxResults, int searchTimeout) {
        this.luceneIndexPath = luceneIndexPath;
        Utils.createParentDirectories(this.luceneIndexPath);

        this.maxResults = maxResults;
        this.searchTimeout = searchTimeout;

        try {
            luceneDirectory = new MMapDirectory(Paths.get(this.luceneIndexPath));
            luceneIndexIsHealthy = true;
        } catch (IOException e) {
            luceneIndexIsHealthy = false;
            String message = ErrorMessageFormat.UNABLE_TO_CREATE_DIR.format(this.luceneIndexPath);
            LOG.error(message, e);
        }
    }

    /**
     * Constructor.  The search timeout is initialized to the default (or configured) value.
     *
     * @param luceneIndexPath  Path to the lucene index files
     * @param maxResults  Maximum number of allowed results in a page
     */
    public LuceneSearchProvider(String luceneIndexPath, int maxResults) {
        this(luceneIndexPath, maxResults, LUCENE_SEARCH_TIMEOUT_MS);
    }

    /**
     * Initializes the `luceneIndexSearcher` if it has not been initialized already.
     * <p>
     * Note that the index searcher cannot be built at construction time, because it needs the dimension and
     * associated key-value store. However, because of a circular dependency between the `SearchProvider` and the
     * `Dimension` classes, we cannot provide the dimension and key-value store to the search provider at
     * construction time.
     */
    private void initializeIndexSearcher() {
        if (luceneIndexSearcher == null) {
            reopenIndexSearcher(true);
        }
    }

    /**
     * Re-open the Index Searcher, opening it for the first time if it's never been opened.
     * <p>
     * This method will attempt to acquire and release a write lock.
     *
     * @param firstTimeThrough  If true, will write an empty index and will then re-open the searcher
     */
    private void reopenIndexSearcher(boolean firstTimeThrough) {
        lock.writeLock().lock();
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
                try (IndexWriter ignored = new IndexWriter(luceneDirectory, indexWriterConfig)) {
                    // Closed automatically by the try-resource block
                } catch (IOException emptyIndexWriteException) {
                    // We can't move past this, so puke
                    luceneIndexIsHealthy = false;
                    String message = String.format("Unable to write empty index to %s:", luceneIndexPath);
                    LOG.error(message, emptyIndexWriteException);
                    throw new RuntimeException(emptyIndexWriteException);
                }
                reopenIndexSearcher(false);
            } else {
                // We've been here before, so puke
                luceneIndexIsHealthy = false;
                String message = String.format("Unable to open index searcher for %s:", luceneIndexPath);
                LOG.error(message, reopenException);
                throw new RuntimeException(reopenException);
            }
        } finally {
            lock.writeLock().unlock();
        }
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
            refreshCardinality();
        }
    }

    @Override
    public int getDimensionCardinality() {
        return Integer.parseInt(
                keyValueStore.getOrDefault(DimensionStoreKeyUtils.getCardinalityKey(), "0")
        );
    }

    @Override
    public Pagination<DimensionRow> findAllDimensionRowsPaged(PaginationParameters paginationParameters) {
        return getResultsPage(new MatchAllDocsQuery(), paginationParameters);
    }

    @Override
    public TreeSet<DimensionRow> findAllOrderedDimensionRows() {
        return new TreeSet<>(findAllDimensionRows());
    }

    @Override
    public boolean isHealthy() {
        return luceneIndexIsHealthy;
    }

    /**
     * Refresh lucene index
     * <p>
     * Query the lucene indexes on key column to see if there are any existing indexes.
     * <p>
     * If yes, then we need to update the indexes.
     * Update is done by first deleting the existing documents and then adding new ones.
     * <p>
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
        try {
            try (IndexWriter luceneIndexWriter = new IndexWriter(luceneDirectory, indexWriterConfig)) {
                // Update the document fields for each row and update the document
                for (String rowId : changedRows.keySet()) {
                    // Get the new row from the pair
                    DimensionRow newDimensionRow = changedRows.get(rowId).getKey();

                    // Update the index
                    updateDimensionRow(doc, dimFieldToLuceneField, luceneIndexWriter, newDimensionRow);
                }

            } catch (IOException e) {
                luceneIndexIsHealthy = false;
                LOG.error("Failed to refresh index for dimension rows", e);
                throw new RuntimeException(e);
                // Commit all the changes to the index (on .close, called by try-resources) and refresh the cardinality
            }
            //This must be outside the try-resources block because it may _also_ need to open an IndexWriter, and
            //opening an IndexWriter involves taking a write lock on lucene, of which there can only be one at a time.
            reopenIndexSearcher(true);
            refreshCardinality();
        } finally {
            lock.writeLock().unlock();
        }
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
            fieldToUpdate.setStringValue(newRow.getOrDefault(field, ""));
        }

        // Build the term to delete the old document by the key value (which should be unique)
        Term keyTerm = new Term(fieldMap.get(dimension.getKey()).name(), newRow.getOrDefault(dimension.getKey(), ""));

        // Update the document by the key term
        writer.updateDocument(keyTerm, luceneDimensionRowDoc);
    }

    @Override
    public void replaceIndex(String newLuceneIndexPathString) {
        LOG.debug(
                "Replacing Lucene indexes at {} for dimension {} with new index at {}",
                luceneDirectory.toString(),
                dimension.getApiName(),
                newLuceneIndexPathString
        );

        lock.writeLock().lock();
        try {
            Path oldLuceneIndexPath = Paths.get(luceneIndexPath);
            String tempDir = oldLuceneIndexPath.resolveSibling(oldLuceneIndexPath.getFileName() + "_old").toString();

            LOG.trace("Moving old Lucene index directory from {} to {} ...", luceneIndexPath, tempDir);
            moveDirEntries(luceneIndexPath, tempDir);

            LOG.trace("Moving all new Lucene indexes from {} to {} ...", newLuceneIndexPathString, luceneIndexPath);
            moveDirEntries(newLuceneIndexPathString, luceneIndexPath);

            LOG.trace(
                    "Deleting {} since new Lucene indexes have been moved away from there and is now empty",
                    newLuceneIndexPathString
            );
            deleteDir(newLuceneIndexPathString);

            LOG.trace("Deleting old Lucene indexes in {} ...", tempDir);
            deleteDir(tempDir);

            reopenIndexSearcher(false);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Moves all files and sub-directories from one location to another.
     * <p>
     * Two locations must exist before calling this method.
     *
     * @param sourceDir  The location where files and sub-directories will be moved from
     * @param destinationDir  The location where files and sub-directories will be moved to
     */
    private static void moveDirEntries(String sourceDir, String destinationDir) {
        Path sourcePath = Paths.get(sourceDir).toAbsolutePath();
        Path destinationPath = Paths.get(destinationDir).toAbsolutePath();

        if (!Files.exists(destinationPath)) {
            try {
                Files.createDirectory(destinationPath);
            } catch (IOException e) {
                LOG.error(ErrorMessageFormat.UNABLE_TO_CREATE_DIR.format(destinationDir));
                throw new RuntimeException(e);
            }
        }

        try {
            Files.walkFileTree(sourcePath, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes basicFileAttributes)
                        throws  IOException {
                    Path destinationDirPath = destinationPath.resolve(sourcePath.relativize(dir));
                    if (!Files.exists(destinationDirPath)) {
                        Files.createDirectory(destinationDirPath);
                        LOG.trace("Creating sub-directory {} under {} ...", dir, destinationDir);
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes basicFileAttributes)
                        throws IOException {
                    Path destinationFileName = destinationPath.resolve(sourcePath.relativize(file));
                    LOG.trace("Moving {} to {}", file, destinationFileName);
                    Files.move(file, destinationFileName);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            LOG.error("I/O error thrown by SimpleFileVisitor method");
            throw new RuntimeException(e);
        }
    }

    /**
     * Deletes a directory and all entries under that directory.
     *
     * @param path  The location of the directory that is to be deleted
     */
    private static void deleteDir(String path) {
        try {
            FileUtils.deleteDirectory(new File(path));
        } catch (IOException e) {
            String message = ErrorMessageFormat.UNABLE_TO_DELETE_DIR.format(path);
            LOG.error(message);
            throw new RuntimeException(message);
        }
    }

    /**
     * Clears the dimension cache, and resets the indices, effectively resetting the SearchProvider to a clean state.
     * <p>
     * Note that this method attempts to acquire a write lock before clearing the index.
     */
    @Override
    public void clearDimension() {
        Set<DimensionRow> dimensionRows = findAllDimensionRows();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(LUCENE_ANALYZER).setRAMBufferSizeMB(BUFFER_SIZE);
        lock.writeLock().lock();
        try {
            try (IndexWriter writer = new IndexWriter(luceneDirectory, indexWriterConfig)) {
                //Remove all dimension data from the store.
                String rowId = dimension.getKey().getName();
                dimensionRows.stream()
                        .map(DimensionRow::getRowMap)
                        .map(map -> map.get(rowId))
                        .map(id -> DimensionStoreKeyUtils.getRowKey(rowId, id))
                        .forEach(keyValueStore::remove);

                //Since Lucene's indices are being dropped, the dimension field stored via the columnKey is becoming
                //stale.
                keyValueStore.remove(DimensionStoreKeyUtils.getColumnKey(dimension.getKey().getName()));
                //The allValues key mapping needs to reflect the fact that we are dropping all dimension data.
                keyValueStore.put(DimensionStoreKeyUtils.getAllValuesKey(), "[]");
                //We're resetting the keyValueStore, so we don't want any stale last updated date floating around.
                keyValueStore.remove(DimensionStoreKeyUtils.getLastUpdatedKey());

                //In addition to clearing the keyValueStore, we also need to delete all of Lucene's segment files.
                writer.deleteAll();
                writer.commit();
            } catch (IOException e) {
                LOG.error(ErrorMessageFormat.FAIL_TO_WIPE_LUCENE_INDEX_DIR.format(luceneDirectory));
                throw new RuntimeException(e);
            }

            //This must be outside the try-resources block because it may _also_ need to open an IndexWriter, and
            //opening an IndexWriter involves taking a write lock on lucene, of which there can only be one at a time.
            reopenIndexSearcher(true);
            refreshCardinality();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Update the cardinality count.
     * <p>
     * Note that this method acquires a read lock to query the lucene index for the number of documents.
     */
    private void refreshCardinality() {
        int numDocs;
        initializeIndexSearcher();
        lock.readLock().lock();
        try {
            numDocs = luceneIndexSearcher.getIndexReader().numDocs();
        } finally {
            lock.readLock().unlock();
        }
        keyValueStore.put(
                DimensionStoreKeyUtils.getCardinalityKey(),
                Integer.toString(numDocs)
        );
    }

    @Override
    public Pagination<DimensionRow> findFilteredDimensionRowsPaged(
            Set<ApiFilter> filters,
            PaginationParameters paginationParameters
    ) {
        return getResultsPage(getFilterQuery(filters), paginationParameters);
    }

    /**
     * Returns a Collector that accumulates boolean queries into a single nested query, and ties all of the sub
     * queries together with the specified {@link BooleanClause.Occur}.
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
     * In-filter operation.
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
     * Startswith-filter operation.
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
     * Contains filter operation.
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

    /**
     * Get query with filter parameters.
     *
     * @param filters  The set of filters
     *
     * @return A query to find all the dimension rows that satisfy the given filter
     */
    private Query getFilterQuery(Set<ApiFilter> filters) {
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

        BooleanQuery booleanQuery = filterQueryBuilder.build();
        LOG.trace("Translated ApiFilters {} into Lucene Query: {}", filters, booleanQuery);
        return booleanQuery;
    }

    /**
     * Returns the desired page of dimension rows found by the specified query with the relevant metadata.
     *
     * @param query  The Lucene query used to locate the desired DimensionRows
     * @param paginationParameters  The parameters defining the pagination (i.e. the number of rows per page, and the
     * desired page)
     * <p>
     * Note that this method _may_ need to acquire and release a write lock if the index searcher needs to be
     * initialized, and it later acquires and released a read lock when querying for dimension data from Lucene.
     *
     * @return The desired page of dimension rows that satisfy the given query
     *
     * @throws PageNotFoundException if the page requested is past the last page of results
     */
    private Pagination<DimensionRow> getResultsPage(Query query, PaginationParameters paginationParameters)
            throws PageNotFoundException {
        int perPage = paginationParameters.getPerPage();
        validatePerPage(perPage);

        TreeSet<DimensionRow> filteredDimRows;
        int documentCount;
        initializeIndexSearcher();
        LOG.trace("Lucene Query {}", query);

        lock.readLock().lock();
        try {
            ScoreDoc[] hits;
            try (TimedPhase timer = RequestLog.startTiming("QueryingLucene")) {
                TopDocs hitDocs = getPageOfData(
                        luceneIndexSearcher,
                        null,
                        query,
                        perPage
                );
                hits = hitDocs.scoreDocs;
                documentCount = hitDocs.totalHits;
                int requestedPageNumber = paginationParameters.getPage(documentCount);
                if (hits.length == 0) {
                    if (requestedPageNumber == 1) {
                        return new SinglePagePagination<>(Collections.emptyList(), paginationParameters, 0);
                    } else {
                        throw new PageNotFoundException(requestedPageNumber, perPage, 0);
                    }
                }
                for (int currentPage = 1; currentPage < requestedPageNumber; currentPage++) {
                    ScoreDoc lastEntry = hits[hits.length - 1];
                    hits = getPageOfData(luceneIndexSearcher, lastEntry, query, perPage).scoreDocs;
                    if (hits.length == 0) {
                        throw new PageNotFoundException(requestedPageNumber, perPage, 0);
                    }
                }
            }

            // convert hits to dimension rows
            try (TimedPhase timer = RequestLog.startTiming("LuceneHydratingDimensionRows")) {
                String idKey = DimensionStoreKeyUtils.getColumnKey(dimension.getKey().getName());
                filteredDimRows = Arrays.stream(hits)
                        .map(
                                hit -> {
                                    try {
                                        return luceneIndexSearcher.doc(hit.doc);
                                    } catch (IOException e) {
                                        LOG.error("Unable to convert hit " + hit);
                                        throw new RuntimeException(e);
                                    }
                                }
                        )
                        .map(document -> document.get(idKey))
                        .map(dimension::findDimensionRowByKeyValue)
                        .collect(Collectors.toCollection(TreeSet::new));
            }
        } finally {
            lock.readLock().unlock();
        }
        return new SinglePagePagination<>(
                Collections.unmodifiableList(filteredDimRows.stream().collect(Collectors.toList())),
                paginationParameters,
                documentCount
        );
    }

    /**
     * Check if perPage exceeds limit of max number of rows to be returned.
     *
     * @param perPage  The number of entries per page
     */
    private void validatePerPage(int perPage) {
        if (perPage > maxResults) {
            String msg = String.format(
                    "Number of rows requested exceeds request limit of %d",
                    maxResults
            );
            throw new RowLimitReachedException(msg);
        }
    }

    /**
     * Returns the requested page of dimension metadata from Lucene.
     * <p>
     * Note that this method acquires and releases a read lock when querying Lucene for data.
     *
     * @param indexSearcher  The service to find the desired dimension metadata in the Lucene index
     * @param lastEntry  The last entry from the previous page of dimension metadata, the indexSearcher will begin its
     * search after this entry (if lastEntry is null, the indexSearcher will begin its search from the beginning)
     * @param query  The Lucene query used to locate the desired dimension metadata
     * @param perPage  The number of entries per page
     *
     * @return The desired page of dimension metadata
     */
    private TopDocs getPageOfData(
            IndexSearcher indexSearcher,
            ScoreDoc lastEntry,
            Query query,
            int perPage
    ) {
        TimeLimitingCollectorManager manager = new TimeLimitingCollectorManager(searchTimeout, lastEntry, perPage);
        lock.readLock().lock();
        try {
            return indexSearcher.search(query, manager);
        } catch (IOException e) {
            String errorMessage = "Unable to find dimension rows for specified page.";
            LOG.error(errorMessage);
            throw new RuntimeException(errorMessage);
        } catch (TimeLimitingCollector.TimeExceededException e) {
            LOG.warn("Lucene query timeout: {}. {}", query, e.getMessage());
            throw new TimeoutException(e.getMessage(), e);
        } finally {
            lock.readLock().unlock();
        }
    }
}
