// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK

import com.yahoo.bard.webservice.data.config.ConfigurationLoader
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig
import com.yahoo.bard.webservice.data.config.dimension.KeyValueStoreDimensionLoader
import com.yahoo.bard.webservice.data.config.dimension.TestDimensions
import com.yahoo.bard.webservice.data.config.metric.TestMetricLoader
import com.yahoo.bard.webservice.data.config.names.TestApiMetricName
import com.yahoo.bard.webservice.data.config.names.TestDruidMetricName
import com.yahoo.bard.webservice.data.config.names.TestDruidTableName
import com.yahoo.bard.webservice.data.config.table.TestTableLoader
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.time.TimeGrain
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation
import com.yahoo.bard.webservice.druid.model.aggregation.SketchCountAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation.ArithmeticPostAggregationFunction
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.druid.model.query.Granularity
import com.yahoo.bard.webservice.druid.util.FieldConverterSupplier
import com.yahoo.bard.webservice.druid.util.FieldConverters
import com.yahoo.bard.webservice.druid.util.SketchFieldConverter
import com.yahoo.bard.webservice.table.LogicalTableDictionary
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.PhysicalTableDictionary
import com.yahoo.bard.webservice.table.TableIdentifier

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class ConfigurationLoaderSpec extends Specification {

    @Shared ConfigurationLoader loader
    @Shared DimensionDictionary dimensionDictionary
    @Shared MetricDictionary metricDictionary
    @Shared LogicalTableDictionary logicalTableDictionary
    @Shared PhysicalTableDictionary physicalTableDictionary

    static FieldConverters oldFieldConverter = FieldConverterSupplier.sketchConverter

    def setupSpec() {
        FieldConverterSupplier.sketchConverter = new SketchFieldConverter()
        LinkedHashSet<DimensionConfig> dimensions = new TestDimensions().getAllDimensionConfigurations()
        loader = new ConfigurationLoader(
                new KeyValueStoreDimensionLoader(dimensions),
                new TestMetricLoader(),
                new TestTableLoader()
        )
        loader.load()

        dimensionDictionary = loader.getDimensionDictionary()
        metricDictionary = loader.getMetricDictionary()
        logicalTableDictionary = loader.getLogicalTableDictionary()
        physicalTableDictionary = loader.getPhysicalTableDictionary()
    }

    def cleanupSpec() {
        FieldConverterSupplier.sketchConverter = oldFieldConverter
    }

    @Unroll
    def "test Dimension dictionary #dim"() {
        expect:
        dimensionDictionary.findByApiName(dim.apiName)?.apiName == dim.apiName

        where:
        dim << dimensionDictionary.findAll()
    }

    def "test table keys"() {
        setup:
        TableIdentifier ti1 = new TableIdentifier("table", DAY)
        TableIdentifier ti2 = new TableIdentifier("table", DAY)
        TableIdentifier ti3 = new TableIdentifier("table", HOUR)

        expect:
        ti1 == ti2
        ti1.hashCode() == ti2.hashCode()
        ti1 != ti3
    }

    def "test sketch metric"() {
        given: "A set of expected aggregations"
        Set<Aggregation> expected = []
        expected << new SketchCountAggregation(
                TestApiMetricName.A_USERS.asName(),
                TestDruidMetricName.USERS.asName(),
                16384
        )

        and: "A metric name"
        String metricName = TestApiMetricName.A_USERS.asName()

        expect: "The aggregations for the TDQ of that named metric are what we expect"
        metricDictionary.get(metricName).templateDruidQuery.aggregations == expected
    }

    def "test longsum metric"() {
        given: "A set of expected aggregations"
        Set<Aggregation> expected = []
        expected << new LongSumAggregation(TestApiMetricName.A_HEIGHT.asName(), TestDruidMetricName.HEIGHT.asName())

        and: "A metric name"
        String metricName = TestApiMetricName.A_HEIGHT.asName()

        expect: "The aggregations for the TDQ of that named metric are what we expect"
        metricDictionary.get(metricName).templateDruidQuery.aggregations == expected
    }

    def "test arithmetic metric"() {
        given: "A set of expected aggregations"
        Aggregation heightAgg = new LongSumAggregation(
                TestApiMetricName.A_HEIGHT.asName(),
                TestDruidMetricName.HEIGHT.asName()
        )
        Aggregation widthAgg = new LongSumAggregation(
                TestApiMetricName.A_WIDTH.asName(),
                TestDruidMetricName.WIDTH.asName()
        )

        Set<Aggregation> expectedAggs = [heightAgg, widthAgg]

        and: "A set of expected post-aggregations"
        Set<PostAggregation> expectedPostAggs = []
        expectedPostAggs << new ArithmeticPostAggregation(
                TestApiMetricName.A_AREA.asName(),
                ArithmeticPostAggregationFunction.MULTIPLY,
                [new FieldAccessorPostAggregation(heightAgg), new FieldAccessorPostAggregation(widthAgg)]
        )

        and: "A metric name"
        String metricName = TestApiMetricName.A_AREA.asName()

        expect: "The aggregations and post-aggregations for the TDQ of that named metric are what we expect"
        metricDictionary.get(metricName).templateDruidQuery.aggregations == expectedAggs
        metricDictionary.get(metricName).templateDruidQuery.postAggregations == expectedPostAggs
    }

    @Unroll
    def "test logical table dictionary load"() {
        setup:
        Granularity<TimeGrain> granularity = tableIdGrain

        expect:
        logicalTableDictionary.get(new TableIdentifier(tableIdName, granularity)).getName() == logicalTableName

        where:
        tableIdName | tableIdGrain    | logicalTableName
        "shapes"    | DAY             | "shapes"
        "shapes"    | WEEK            | "shapes"
        "shapes"    | MONTH           | "shapes"
    }

    def "test existence of multi named dimension in dimension dictionary"() {
        setup:
        Dimension dimension = dimensionDictionary.findByApiName("other")

        expect:
        dimension.getApiName() == "other"
    }

    def "test fetching of physicalTable by its name"() {
        setup: "fetch a physical table by its druid name"
        PhysicalTable physicalTable = physicalTableDictionary.get(TestDruidTableName.ALL_SHAPES.asName())

        expect: "fetched table has the same name as that requested"
        physicalTable.getName() == TestDruidTableName.ALL_SHAPES.asName()
    }
}
