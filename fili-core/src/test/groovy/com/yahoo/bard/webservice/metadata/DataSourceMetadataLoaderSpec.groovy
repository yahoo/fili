// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata

import static com.yahoo.bard.webservice.data.Columns.DIMENSIONS
import static com.yahoo.bard.webservice.data.Columns.METRICS

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.config.names.TestApiDimensionName
import com.yahoo.bard.webservice.data.config.names.TestApiMetricName
import com.yahoo.bard.webservice.data.config.names.TestDruidTableName
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.druid.client.DruidWebService
import com.yahoo.bard.webservice.druid.client.SuccessCallback
import com.yahoo.bard.webservice.druid.model.datasource.DataSource
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.models.druid.client.impl.TestDruidWebService
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.ConcretePhysicalTable
import com.yahoo.bard.webservice.table.PhysicalTableDictionary

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Interval
import org.joda.time.format.DateTimeFormat

import spock.lang.Shared
import spock.lang.Specification

class DataSourceMetadataLoaderSpec extends Specification {
    private static final ObjectMappersSuite MAPPERS = new ObjectMappersSuite()

    String tableName = TestDruidTableName.ALL_PETS.asName();

    Interval interval1 = Interval.parse("2015-01-01T00:00:00.000Z/2015-01-02T00:00:00.000Z")
    Interval interval2 = Interval.parse("2015-01-02T00:00:00.000Z/2015-01-03T00:00:00.000Z")
    Interval interval3 = Interval.parse("2015-01-03T00:00:00.000Z/2015-01-04T00:00:00.000Z")
    Interval interval123 = Interval.parse("2015-01-01T00:00:00.000Z/2015-01-04T00:00:00.000Z")

    String version1 = DateTimeFormat.fullDateTime().print(DateTime.now().minusDays(1))
    String version2 = DateTimeFormat.fullDateTime().print(DateTime.now())

    TestApiDimensionName dim1 = TestApiDimensionName.BREED
    TestApiDimensionName dim2 = TestApiDimensionName.SPECIES
    TestApiDimensionName dim3 = TestApiDimensionName.SEX

    List<String> dimensions123 = [dim1, dim2, dim3]*.asName()
    List<String> dimensions13 = [dim1, dim3]*.asName()

    List<String> quotedDimensions = dimensions13.collect() { '"' + it + '"'}

    TestApiMetricName met1 = TestApiMetricName.A_ROW_NUM
    TestApiMetricName met2 = TestApiMetricName.A_LIMBS
    TestApiMetricName met3 = TestApiMetricName.A_DAY_AVG_LIMBS

    List<String> metrics123 = [met1, met2, met3]*.asName()
    List<String> metrics13 = [met1, met3]*.asName()

    Integer binversion1 = 9
    long size1 = 1024
    long size2 = 512

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

    String fullDataSourceMetadataJson =
           """{
            "name": "$tableName",
            "properties": {},
            "segments": [
                    ${[
                            generateSegment(tableName, interval1, version1, dimensions123.join(','), metrics123.join(','), 0, 2, binversion1, size1),
                            generateSegment(tableName, interval1, version2, dimensions123.join(','), metrics123.join(','), 1, 2, binversion1, size2),
                            generateSegment(tableName, interval2, version1, dimensions123.join(','), metrics123.join(','), 0, 2, binversion1, size1),
                            generateSegment(tableName, interval2, version2, dimensions123.join(','), metrics123.join(','), 1, 2, binversion1, size2),
                            generateSegment(tableName, interval3, version1, dimensions123.join(','), metrics123.join(','), 0, 2, binversion1, size1),
                            generateSegment(tableName, interval3, version2, dimensions123.join(','), metrics123.join(','), 1, 2, binversion1, size2),
                            generateSegment_9_1(tableName, interval3, version2, dimensions123.join(','), metrics123.join(','), 0, 2, [], binversion1, size2),
                            generateSegment_9_1(tableName, interval3, version2, dimensions123.join(','), metrics123.join(','), 1, 2, quotedDimensions, binversion1, size2)
                    ].join(',')}
                ]
            }"""

    String gappyDataSourceMetadataJson =
           """{
            "name": "$tableName",
            "properties": {},
            "segments": [
                   ${[
                            generateSegment(tableName, interval1, version1, dimensions123.join(','), metrics123.join(','), 0, 2, binversion1, size1),
                            generateSegment(tableName, interval1, version2, dimensions123.join(','), metrics123.join(','), 1, 2, binversion1, size2),
                            generateSegment(tableName, interval2, version1, dimensions13.join(','), metrics13.join(','), 0, 2, binversion1, size1),
                            generateSegment(tableName, interval2, version2, dimensions13.join(','), metrics13.join(','), 1, 2, binversion1, size2),
                            generateSegment(tableName, interval3, version1, dimensions123.join(','), metrics123.join(','), 0, 2, binversion1, size1),
                            generateSegment(tableName, interval3, version2, dimensions123.join(','), metrics123.join(','), 1, 2, binversion1, size2),
                            generateSegment_9_1(tableName, interval3, version1, dimensions123.join(','), metrics123.join(','), 0, 2, [], binversion1, size1),
                            generateSegment_9_1(tableName, interval3, version2, dimensions123.join(','), metrics123.join(','), 1, 2, quotedDimensions, binversion1, size2)
                   ].join(',')}
                ]
            }"""

    JerseyTestBinder jtb

    PhysicalTableDictionary tableDict
    DataSourceMetadataService metadataService
    SegmentIntervalsHashIdGenerator segmentSetIdGenerator
    DimensionDictionary dimensionDict
    TestDruidWebService druidWS = new TestDruidWebService()
    Map<Column, Set<Interval>> expectedIntervalsMap

    @Shared
    DateTimeZone currentTZ

    def setupSpec() {
        currentTZ = DateTimeZone.getDefault()
        DateTimeZone.setDefault(DateTimeZone.UTC);
    }

    def shutdownSpec() {
        DateTimeZone.setDefault(currentTZ)
    }

    def setup() {
        jtb = new JerseyTestBinder()
        segmentSetIdGenerator = jtb.testBinderFactory.querySigningService
        metadataService = jtb.testBinderFactory.getDataSourceMetadataService()
        dimensionDict = jtb.configurationLoader.dimensionDictionary
        tableDict = jtb.configurationLoader.physicalTableDictionary
        druidWS.jsonResponse = {fullDataSourceMetadataJson}

        expectedIntervalsMap = [:]
        dimensions123.each {
            expectedIntervalsMap.put(new DimensionColumn(dimensionDict.findByApiName(it)), [interval123] as Set)
        }
        metrics123.each { expectedIntervalsMap.put(new MetricColumn(it), [interval123] as Set) }
    }

    def cleanup() {
        jtb.tearDown()
    }

    def "test whether DataSourceMetadataLoader loads any metadata segments"() {
        setup: "run the loader"
        DataSourceMetadataLoader loader = new DataSourceMetadataLoader(
                tableDict,
                metadataService,
                druidWS,
                MAPPERS.mapper
        )
        DataSource dataSource = Mock(DataSource)
        dataSource.getNames() >> ([tableName] as Set)
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
        DataSourceMetadataLoader loader = new DataSourceMetadataLoader(
                tableDict,
                localMetadataService,
                druidWS,
                MAPPERS.mapper
        )
        druidWS.jsonResponse = {gappyDataSourceMetadataJson}
        ConcretePhysicalTable table = Mock(ConcretePhysicalTable)
        table.getFactTableName() >> "test"
        DataSourceMetadata capture

        when: "JSON metadata return successfully"
        SuccessCallback success = loader.buildDataSourceMetadataSuccessCallback(table)
        success.invoke(MAPPERS.mapper.readTree(gappyDataSourceMetadataJson))

        then: "the segment metadata are loaded to the metadata service as expected"
        1 * localMetadataService.update(table, _) >> { physicalTable, dataSourceMetadata ->
            capture = dataSourceMetadata
        }
        def intervals = DataSourceMetadata.getIntervalLists(capture)
        intervals.get(DIMENSIONS).containsKey(dim3.asName())
        intervals.get(DIMENSIONS).get(dim3.asName()).size() == 1
        intervals.get(METRICS).get(met2.asName()).size() == 2
        capture.name == tableName
        capture.properties == [:]
        capture.segments[0].dimensions == dimensions123
        capture.segments[1].size == size2
        capture.segments[1].shardSpec.partitionNum == 1
    }

    def "Test queryDataSourceMetadata builds callbacks and sends query"() {
        setup: "instantiate the loader"
        DruidWebService testWs = Mock(DruidWebService)
        DataSourceMetadataLoader loader = new DataSourceMetadataLoader(
                tableDict,
                metadataService,
                testWs,
                MAPPERS.mapper
        )
        ConcretePhysicalTable table = Mock(ConcretePhysicalTable)
        table.getFactTableName() >> "test"

        when: "loader issues a metadata query"
        loader.queryDataSourceMetadata(table)

        then: "the query is issued to the webservice that was specified to query the druid metadata endpoint"
        1 * testWs.getJsonObject(_, _, _, _)
    }
}
