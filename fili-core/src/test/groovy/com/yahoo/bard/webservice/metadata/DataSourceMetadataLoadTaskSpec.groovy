// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata

import static com.yahoo.bard.webservice.data.Columns.DIMENSIONS
import static com.yahoo.bard.webservice.data.Columns.METRICS

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.config.names.DataSourceName
import com.yahoo.bard.webservice.data.config.names.TestApiDimensionName
import com.yahoo.bard.webservice.data.config.names.TestApiMetricName
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.druid.client.DruidWebService
import com.yahoo.bard.webservice.druid.client.SuccessCallback
import com.yahoo.bard.webservice.druid.model.datasource.DataSource
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.models.druid.client.impl.TestDruidWebService
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.ConstrainedTable
import com.yahoo.bard.webservice.table.PhysicalTableDictionary
import com.yahoo.bard.webservice.table.StrictPhysicalTable

import com.fasterxml.jackson.databind.InjectableValues

import org.joda.time.Interval

import io.druid.timeline.DataSegment

class DataSourceMetadataLoadTaskSpec extends BaseDataSourceMetadataSpec {

    private static final ObjectMappersSuite MAPPERS = new ObjectMappersSuite()

    Interval interval1
    Interval interval2
    Interval interval3

    List<String> dimensions13
    List<String> quotedDimensions
    List<String> metrics123
    List<String> metrics13

    String fullDataSourceMetadataJson
    String gappyDataSourceMetadataJson

    JerseyTestBinder jtb

    PhysicalTableDictionary tableDict
    DataSourceMetadataService metadataService
    SegmentIntervalsHashIdGenerator segmentSetIdGenerator
    DimensionDictionary dimensionDict
    TestDruidWebService druidWS = new TestDruidWebService()
    Map<Column, Set<Interval>> expectedIntervalsMap

    @Override
    def childSetupSpec() {
        tableName = generateTableName()
        intervals = generateIntervals()
        dimensions = generateDimensions()
        metrics = generateMetrics()
        versions = generateVersions()
        sizes = generateSizes()
        binaryVersions = generateBinaryVersions()
    }

    @Override
    Map<String, Interval> generateIntervals() {
        [
                "interval1": Interval.parse("2015-01-01T00:00:00.000Z/2015-01-02T00:00:00.000Z"),
                "interval2": Interval.parse("2015-01-02T00:00:00.000Z/2015-01-03T00:00:00.000Z"),
                "interval3": Interval.parse("2015-01-03T00:00:00.000Z/2015-01-04T00:00:00.000Z"),
                "interval123": Interval.parse("2015-01-01T00:00:00.000Z/2015-01-04T00:00:00.000Z")
        ]
    }

    def setup() {
        childSetupSpec()

        InjectableValues injectableValues = new InjectableValues.Std()
        MAPPERS.mapper.setInjectableValues(injectableValues)
        injectableValues.addValue(DataSegment.PruneLoadSpecHolder.name, DataSegment.PruneLoadSpecHolder.DEFAULT)

        dimensions13 = [
                dimensions.(TestApiDimensionName.BREED.asName()).asName(),
                dimensions.(TestApiDimensionName.SEX.asName()).asName()
        ]
        quotedDimensions = dimensions13.collect() { /"$it"/ }

        metrics123 = [
                metrics.(TestApiMetricName.A_ROW_NUM.asName()).asName(),
                metrics.(TestApiMetricName.A_LIMBS.asName()).asName(),
                metrics.(TestApiMetricName.A_DAY_AVG_LIMBS.asName()).asName()
        ]
        metrics13 = [TestApiMetricName.A_ROW_NUM.asName(), TestApiMetricName.A_DAY_AVG_LIMBS.asName()]

        interval1 = intervals.interval1
        interval2 = intervals.interval2
        interval3 = intervals.interval3

        fullDataSourceMetadataJson =
                """{
                       "name": "$tableName",
                       "properties": {},
                       "segments": [
                           ${[
                               generateSegment(tableName, interval1, versions.version1, dimensions.keySet().join(','), metrics123.join(','), 0, 2, binaryVersions.binaryVersion1, sizes.size1),
                               generateSegment(tableName, interval1, versions.version2, dimensions.keySet().join(','), metrics123.join(','), 1, 2, binaryVersions.binaryVersion1, sizes.size2),
                               generateSegment(tableName, interval1, versions.version1, dimensions.keySet().join(','), metrics123.join(','), 0, 2, binaryVersions.binaryVersion1, sizes.size1),
                               generateSegment(tableName, interval2, versions.version2, dimensions.keySet().join(','), metrics123.join(','), 1, 2, binaryVersions.binaryVersion1, sizes.size2),
                               generateSegment(tableName, interval3, versions.version1, dimensions.keySet().join(','), metrics123.join(','), 0, 2, binaryVersions.binaryVersion1, sizes.size1),
                               generateSegment(tableName, interval3, versions.version2, dimensions.keySet().join(','), metrics123.join(','), 1, 2, binaryVersions.binaryVersion1, sizes.size2),
                               generateSegment_9_1(tableName, interval3, versions.version2, dimensions.keySet().join(','), metrics123.join(','), 0, 2, [], binaryVersions.binaryVersion1, sizes.size2),
                               generateSegment_9_1(tableName, interval3, versions.version2, dimensions.keySet().join(','), metrics123.join(','), 1, 2, quotedDimensions, binaryVersions.binaryVersion1, sizes.size2)
                           ].join(',')}
                       ]
                }"""

        gappyDataSourceMetadataJson =
                """{
                      "name": "$tableName",
                      "properties": {},
                      "segments": [
                          ${[
                              generateSegment(tableName, interval1, versions.version1, dimensions.keySet().join(','), metrics123.join(','), 0, 2, binaryVersions.binaryVersion1, sizes.size1),
                              generateSegment(tableName, interval1, versions.version2, dimensions.keySet().join(','), metrics123.join(','), 1, 2, binaryVersions.binaryVersion1, sizes.size2),
                              generateSegment(tableName, interval2, versions.version1, dimensions13.join(','), metrics13.join(','), 0, 2, binaryVersions.binaryVersion1, sizes.size1),
                              generateSegment(tableName, interval2, versions.version2, dimensions13.join(','), metrics13.join(','), 1, 2, binaryVersions.binaryVersion1, sizes.size2),
                              generateSegment(tableName, interval3, versions.version1, dimensions.keySet().join(','), metrics123.join(','), 0, 2, binaryVersions.binaryVersion1, sizes.size1),
                              generateSegment(tableName, interval3, versions.version2, dimensions.keySet().join(','), metrics123.join(','), 1, 2, binaryVersions.binaryVersion1, sizes.size2),
                              generateSegment_9_1(tableName, interval3, versions.version1, dimensions.keySet().join(','), metrics123.join(','), 0, 2, [], binaryVersions.binaryVersion1, sizes.size1),
                              generateSegment_9_1(tableName, interval3, versions.version2, dimensions.keySet().join(','), metrics123.join(','), 1, 2, quotedDimensions, binaryVersions.binaryVersion1, sizes.size2)
                          ].join(',')}
                      ]
                }"""

        jtb = new JerseyTestBinder()
        segmentSetIdGenerator = jtb.testBinderFactory.querySigningService
        metadataService = jtb.testBinderFactory.getDataSourceMetadataService()
        dimensionDict = jtb.configurationLoader.dimensionDictionary
        tableDict = jtb.configurationLoader.physicalTableDictionary
        druidWS.jsonResponse = {fullDataSourceMetadataJson}

        Interval interval123 = intervals.interval123

        expectedIntervalsMap = dimensions
                .collect {dimensionDict.findByApiName(it.key)}
                .collectEntries{[(it): [interval123] as Set]}
        metrics123.collect {new MetricColumn(it)}.each {expectedIntervalsMap[it] = [interval123] as Set}
    }

    def cleanup() {
        jtb.tearDown()
    }

    def "test whether DataSourceMetadataLoader loads any metadata segments"() {
        setup: "run the loader"
        DataSourceMetadataLoadTask loader = new DataSourceMetadataLoadTask(
                tableDict,
                metadataService,
                druidWS,
                MAPPERS.mapper
        )
        DataSource dataSource = Mock(DataSource)
        dataSource.physicalTable >> Mock(ConstrainedTable) {
            getDataSourceNames() >> ([DataSourceName.of(tableName)] as Set)
        }

        DruidAggregationQuery<?> query = Mock(DruidAggregationQuery)
        query.intervals >> [interval1, interval2]
        query.innermostQuery >> query
        query.dataSource >> dataSource

        loader.run()

        expect: "cache gets loaded as expected"
        segmentSetIdGenerator.getSegmentSetId(query) != OptionalInt.empty()
    }

    def "Test datasource metadata can deserialize JSON correctly"() {
        setup: "instantiate the loader"
        DataSourceMetadataService localMetadataService = Mock(DataSourceMetadataService)
        DataSourceMetadataLoadTask loader = new DataSourceMetadataLoadTask(
                tableDict,
                localMetadataService,
                druidWS,
                MAPPERS.mapper
        )
        druidWS.jsonResponse = {gappyDataSourceMetadataJson}
        StrictPhysicalTable table = Mock(StrictPhysicalTable)
        table.dataSourceName >> DataSourceName.of("test")
        DataSourceMetadata capture

        when: "JSON metadata return successfully"
        SuccessCallback success = loader.buildDataSourceMetadataSuccessCallback(table.dataSourceName)
        success.invoke(MAPPERS.mapper.readTree(gappyDataSourceMetadataJson))

        then: "the segment metadata are loaded to the metadata service as expected"
        1 * localMetadataService.update(table.dataSourceName, _ as DataSourceMetadata) >> { physicalTable, dataSourceMetadata ->
            capture = dataSourceMetadata
        }
        def intervalLists = DataSourceMetadata.getIntervalLists(capture)
        intervalLists.get(DIMENSIONS).containsKey(dimensions.(TestApiDimensionName.SEX.asName()).asName())
        intervalLists.get(DIMENSIONS).get(dimensions.(TestApiDimensionName.SEX.asName()).asName()).size() == 1
        intervalLists.get(METRICS).get(metrics.(TestApiMetricName.A_LIMBS.asName()).asName()).size() == 2
        capture.name == tableName
        capture.properties == [:]
        capture.segments[0].dimensions.toSet().sort() == dimensions.keySet().sort()
        capture.segments[1].size == sizes.size2
        capture.segments[1].shardSpec.partitionNum == 1
    }

    def "Test queryDataSourceMetadata builds callbacks and sends query"() {
        setup: "instantiate the loader"
        DruidWebService testWs = Mock(DruidWebService)
        DataSourceMetadataLoadTask loader = new DataSourceMetadataLoadTask(
                tableDict,
                metadataService,
                testWs,
                MAPPERS.mapper
        )
        StrictPhysicalTable table = Mock(StrictPhysicalTable)
        table.dataSourceName >> DataSourceName.of("test")

        when: "loader issues a metadata query"
        loader.queryDataSourceMetadata(table.dataSourceName)

        then: "the query is issued to the webservice that was specified to query the druid metadata endpoint"
        1 * testWs.getJsonObject(_, _, _, _)
    }

    def generateSegment(tableName, interval, version, dimensions, metrics, partitionNum, partitions, binVersion, size) {
        return """{
                        "dataSource": "$tableName",
                        "interval": "$interval",
                        "version": "$version",
                        "loadSpec": { },
                        "dimensions": "$dimensions",
                        "metrics": "$metrics",
                        "shardSpec": {
                            "type": "hashed",
                            "partitionNum": $partitionNum,
                            "partitions": $partitions
                        },
                        "binaryVersion": $binVersion,
                        "size": $size,
                        "identifier": ""
        }"""
    }

    def generateSegment_9_1(tableName, interval, version, dimensions, metrics, partitionNum, partitions, List partitionDimensions, binVersion, size) {
        return """{
                        "dataSource": "$tableName",
                        "interval": "$interval",
                        "version": "$version",
                        "loadSpec": { },
                        "dimensions": "$dimensions",
                        "metrics": "$metrics",
                        "shardSpec": {
                            "type": "hashed",
                            "partitionNum": $partitionNum,
                            "partitions": $partitions,
                            "partitionDimensions" : $partitionDimensions
                        },
                        "binaryVersion": $binVersion,
                        "size": $size,
                        "identifier": ""
        }"""
    }
}
