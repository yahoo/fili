// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.DRUID_METADATA_READ_ERROR;
import static javax.ws.rs.core.Response.Status.NO_CONTENT;

import com.yahoo.bard.webservice.application.LoadTask;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.client.SuccessCallback;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;
import com.yahoo.bard.webservice.table.SingleDataSourcePhysicalTable;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Singleton;

/**
 * Datasource metadata loader sends requests to the druid datasource metadata endpoint ('datasources') and returns the
 * lists of available data segments for each datasource. It then builds Datasource Metadata objects which pivot this
 * data into columns of intervals and then updates the {@link DataSourceMetadataService}.
 * <p>
 * Note that this uses the segmentMetadata query that touches the coordinator.
 */
@Singleton
public class DataSourceMetadataLoadTask extends LoadTask<Boolean> {

    private static final Logger LOG = LoggerFactory.getLogger(DataSourceMetadataLoadTask.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    public static final String DATASOURCE_METADATA_QUERY_FORMAT = "/datasources/%s?full";

     /**
     * Parameter specifying the period of the segment metadata loader, in milliseconds.
     */
    public static final String DRUID_SEG_LOADER_TIMER_DURATION_KEY =
            SYSTEM_CONFIG.getPackageVariableName("druid_seg_loader_timer_duration");

    /**
     * Parameter specifying the delay before the first run of the segment metadata loader, in milliseconds.
     */
    public static final String DRUID_SEG_LOADER_TIMER_DELAY_KEY =
            SYSTEM_CONFIG.getPackageVariableName("druid_seg_loader_timer_delay");

    private final DruidWebService druidWebService;
    private final PhysicalTableDictionary physicalTableDictionary;
    private final DataSourceMetadataService metadataService;
    private final AtomicReference<DateTime> lastRunTimestamp;
    private final ObjectMapper mapper;
    private final FailureCallback failureCallback;

    /**
     * Datasource metadata loader fetches data from the druid coordinator and updates the datasource metadata service.
     *
     * @param physicalTableDictionary  The physical tables with data sources to update
     * @param metadataService  The service that will store the metadata loaded by this loader
     * @param druidWebService  The druid webservice to query
     * @param mapper  Object mapper to parse druid metadata
     */
    public DataSourceMetadataLoadTask(
            PhysicalTableDictionary physicalTableDictionary,
            DataSourceMetadataService metadataService,
            DruidWebService druidWebService,
            ObjectMapper mapper
    ) {
        super(
                DataSourceMetadataLoadTask.class.getSimpleName(),
                SYSTEM_CONFIG.getLongProperty(DRUID_SEG_LOADER_TIMER_DELAY_KEY, 0),
                SYSTEM_CONFIG.getLongProperty(
                        DRUID_SEG_LOADER_TIMER_DURATION_KEY,
                        TimeUnit.MINUTES.toMillis(1)
                )
        );

        this.physicalTableDictionary = physicalTableDictionary;
        this.metadataService = metadataService;
        this.druidWebService = druidWebService;
        this.mapper = mapper;
        this.failureCallback = getFailureCallback();
        this.lastRunTimestamp = new AtomicReference<>();
    }

    @Override
    public void runInner() {
        physicalTableDictionary.values().stream()
                .map(PhysicalTable::getDataSourceNames)
                .flatMap(Set::stream)
                .distinct()
                .peek(dataSourceName -> LOG.trace("Querying metadata for datasource: {}", dataSourceName))
                .forEach(this::queryDataSourceMetadata);
        lastRunTimestamp.set(DateTime.now());
    }

    /**
     * Queries Druid for updated datasource metadata and then updates the datasource metadata service.
     *
     * @param table  The physical table to be updated.
     *
     * @deprecated  Pass the DataSourceName directly, rather than via the PhysicalTable
     */
    @Deprecated
    protected void queryDataSourceMetadata(SingleDataSourcePhysicalTable table) {
        queryDataSourceMetadata(table.getDataSourceName());
    }

    /**
     * Queries Druid for updated datasource metadata and then updates the datasource metadata service.
     *
     * @param dataSourceName  The data source to be updated.
     */
    protected void queryDataSourceMetadata(DataSourceName dataSourceName) {
        String resourcePath = String.format(DATASOURCE_METADATA_QUERY_FORMAT, dataSourceName.asName());

        // Success callback will update datasource metadata on success
        SuccessCallback success = buildDataSourceMetadataSuccessCallback(dataSourceName);
        HttpErrorCallback errorCallback = getErrorCallback(dataSourceName);
        druidWebService.getJsonObject(success, errorCallback, failureCallback, resourcePath);
    }

    /**
     * Callback to parse druid datasource metadata response.
     * <p>
     * Typical druid datasource metadata response:
     * <pre>
     *  """
     *  {
     *      "name": "tableName",
     *      "properties": { },
     *      "segments": [
     *          {
     *              "dataSource": "tableName",
     *              "interval": "2015-01-01T00:00:00.000Z/2015-01-02T00:00:00.000Z",
     *              "version": "2015-01-15T18:08:20.435Z",
     *              "loadSpec": {
     *                  "type": "hdfs",
     *                  "path": "hdfs:/some_hdfs_URL/tableName/.../index.zip"
     *              },
     *              "dimensions": "color", "shape",
     *              "metrics": "height", "width",
     *              "shardSpec": {
     *                  "type":"hashed",
     *                  "partitionNum": 0,
     *                  "partitions": 2
     *              },
     *              "binaryVersion":9,
     *              "size":1024,
     *              "identifier":"tableName_2015-01-01T00:00:00.000Z_2015-01-02T00:00:00.000Z_2015-02-15T18:08:20.435Z"
     *          },
     *          {
     *              "dataSource": "tableName",
     *              "interval": "2015-01-01T00:00:00.000Z/2015-01-02T00:00:00.000Z",
     *              "version": "2015-02-01T07:02:05.912Z",
     *              "loadSpec": {
     *                  "type": "hdfs",
     *                  "path": "hdfs:/some_hdfs_URL/tableName/.../index.zip"
     *              },
     *              "dimensions": "color", "shape",
     *              "metrics": "height", "width",
     *              "shardSpec": {
     *                  "type":"hashed",
     *                  "partitionNum": 1,
     *                  "partitions": 2
     *              },
     *              "binaryVersion":9,
     *              "size":512,
     *              "identifier":"tableName_2015-01-01T00:00:00.000Z_2015-01-02T00:00:00.000Z_2015-02-01T07:02:05.912Z"
     *          }
     *      ]
     *   }"""
     * </pre>
     *
     * @param table  The table to inject into this callback.
     *
     * @return The callback itself.
     *
     * @deprecated  Pass the DataSourceName directly, rather than via the PhysicalTable
     */
    @Deprecated
    protected final SuccessCallback buildDataSourceMetadataSuccessCallback(SingleDataSourcePhysicalTable table) {
        return buildDataSourceMetadataSuccessCallback(table.getDataSourceName());
    }

    /**
     * Callback to parse druid datasource metadata response.
     * <p>
     * Typical druid datasource metadata response:
     * <pre>
     *  """
     *  {
     *      "name": "tableName",
     *      "properties": { },
     *      "segments": [
     *          {
     *              "dataSource": "tableName",
     *              "interval": "2015-01-01T00:00:00.000Z/2015-01-02T00:00:00.000Z",
     *              "version": "2015-01-15T18:08:20.435Z",
     *              "loadSpec": {
     *                  "type": "hdfs",
     *                  "path": "hdfs:/some_hdfs_URL/tableName/.../index.zip"
     *              },
     *              "dimensions": "color", "shape",
     *              "metrics": "height", "width",
     *              "shardSpec": {
     *                  "type":"hashed",
     *                  "partitionNum": 0,
     *                  "partitions": 2
     *              },
     *              "binaryVersion":9,
     *              "size":1024,
     *              "identifier":"tableName_2015-01-01T00:00:00.000Z_2015-01-02T00:00:00.000Z_2015-02-15T18:08:20.435Z"
     *          },
     *          {
     *              "dataSource": "tableName",
     *              "interval": "2015-01-01T00:00:00.000Z/2015-01-02T00:00:00.000Z",
     *              "version": "2015-02-01T07:02:05.912Z",
     *              "loadSpec": {
     *                  "type": "hdfs",
     *                  "path": "hdfs:/some_hdfs_URL/tableName/.../index.zip"
     *              },
     *              "dimensions": "color", "shape",
     *              "metrics": "height", "width",
     *              "shardSpec": {
     *                  "type":"hashed",
     *                  "partitionNum": 1,
     *                  "partitions": 2
     *              },
     *              "binaryVersion":9,
     *              "size":512,
     *              "identifier":"tableName_2015-01-01T00:00:00.000Z_2015-01-02T00:00:00.000Z_2015-02-01T07:02:05.912Z"
     *          }
     *      ]
     *   }"""
     * </pre>
     *
     * @param dataSourceName  The datasource name to inject into this callback.
     *
     * @return The callback itself.
     */
    protected SuccessCallback buildDataSourceMetadataSuccessCallback(DataSourceName dataSourceName) {
        return rootNode -> {
            try {
                metadataService.update(dataSourceName, mapper.treeToValue(rootNode, DataSourceMetadata.class));
            } catch (IOException e) {
                LOG.error(DRUID_METADATA_READ_ERROR.format(dataSourceName.asName()), e);
                throw new UnsupportedOperationException(DRUID_METADATA_READ_ERROR.format(dataSourceName.asName()), e);
            }
        };
    }

    /**
     * Return when this loader ran most recently.
     *
     * @return The date and time of the most recent execution of this loader.
     */
    public DateTime getLastRunTimestamp() {
        return lastRunTimestamp.get();
    }

    /**
     * Get a default callback for an http error.
     *
     * @param table  The PhysicalTable that the error callback will relate to.
     *
     * @return A newly created http error callback object.
     *
     * @deprecated  Pass the DataSourceName directly, rather than via the PhysicalTable
     */
    @Deprecated
    protected HttpErrorCallback getErrorCallback(SingleDataSourcePhysicalTable table) {
        return getErrorCallback(table.getDataSourceName());
    }

    /**
     * Get a default callback for an http error.
     *
     * @param dataSourceName  The data source that the error callback will relate to.
     *
     * @return A newly created http error callback object.
     */
    protected HttpErrorCallback getErrorCallback(DataSourceName dataSourceName) {
        return new TaskHttpErrorCallback(dataSourceName);
    }

    /**
     * Defines the callback for http errors.
     */
    private final class TaskHttpErrorCallback extends LoadTask<?>.TaskHttpErrorCallback {
        private final DataSourceName dataSourceName;

        /**
         * Constructor.
         *
         * @param table  PhysicalTable that this error callback is tied to
         *
         * @deprecated  Pass the DataSourceName directly, rather than via the PhysicalTable
         */
        @Deprecated
        TaskHttpErrorCallback(SingleDataSourcePhysicalTable table) {
            this(table.getDataSourceName());
        }

        /**
         * Constructor.
         *
         * @param dataSourceName  Data source that this error callback is tied to
         */
        TaskHttpErrorCallback(DataSourceName dataSourceName) {
            this.dataSourceName = dataSourceName;
        }

        @Override
        public void invoke(int statusCode, String reason, String responseBody) {
            // No Content is an expected but A-typical response.
            // Usually, it means Druid knows about the data source, but no segments have been loaded
            if (statusCode == NO_CONTENT.getStatusCode()) {
                String msg = String.format(
                        "Druid returned 204 NO CONTENT when loading metadata for the '%s' datasource. While not an " +
                                "error, it is unusual for a Druid data source to report having no data in it. Please " +
                                "verify that your cluster is healthy.",
                        dataSourceName.asName()
                );
                LOG.warn(msg);
                metadataService.update(
                        dataSourceName,
                        new DataSourceMetadata(dataSourceName.asName(), Collections.emptyMap(), Collections.emptyList())
                );
            } else {
                String msg = String.format(
                        "%s: HTTP error while trying to load metadata for data source: %s - %d %s, Response body: %s",
                        getName(),
                        dataSourceName.asName(),
                        statusCode,
                        reason,
                        responseBody
                );
                LOG.error(msg);
            }
        }
    }
}
