// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data

import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.BREED
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.COLOR
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.OTHER
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.SHAPE
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.SIZE
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.SPECIES
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static org.joda.time.DateTimeZone.UTC

import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig
import com.yahoo.bard.webservice.data.config.dimension.TestLookupDimensions
import com.yahoo.bard.webservice.data.config.dimension.TestRegisteredLookupDimensions
import com.yahoo.bard.webservice.data.config.names.ApiMetricName
import com.yahoo.bard.webservice.data.config.names.DataSourceName
import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.LookupDimension
import com.yahoo.bard.webservice.data.dimension.impl.RegisteredLookupDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricColumn
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.time.TimeGrain
import com.yahoo.bard.webservice.data.volatility.DefaultingVolatileIntervalsService
import com.yahoo.bard.webservice.data.volatility.VolatileIntervalsFunction
import com.yahoo.bard.webservice.data.volatility.VolatileIntervalsService
import com.yahoo.bard.webservice.druid.model.query.AllGranularity
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.metadata.TestDataSourceMetadataService
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.StrictPhysicalTable
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.LogicalTableDictionary
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.table.TableIdentifier
import com.yahoo.bard.webservice.table.availability.StrictAvailability
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.DateTimeZone
import org.joda.time.Interval

public class QueryBuildingTestingResources {

    // Aggregatable dimensions, numbered for identification
    public Dimension d1, d2, d3, d4, d5

    // Non-aggregatable dimensions, numbered for identification
    public Dimension d6, d7, d8, d9, d10, d11, d12, d13

    // Logical metrics, numbered for identification
    public LogicalMetric m1, m2, m3, m4, m5, m6

    // Some intervals
    public Interval interval1, interval2, interval3

    // tables are enumerated by dimension set number, d or h for day or hour
    public StrictPhysicalTable t1h, t1d, t1hShort, t2h, t3d, t4h1, t4h2, t4d1, t4d2, t5h

    // table used to test ordering, empty has no availability, partial some, whole largest availability
    public StrictPhysicalTable emptyFirst, partialSecond, wholeThird, emptyLast

    // Tables with not aggregatable dimensions, numbers indicate the dimension set
    public StrictPhysicalTable tna1236d, tna1237d, tna167d, tna267d

    // Tables with volatile hour and volatile day
    public StrictPhysicalTable volatileHourTable, volatileDayTable

    public VolatileIntervalsService volatileIntervalsService

    // Table Groups h/d for hour day, number to indicate which concrete tables comprise them, 'na' for non-aggregatable
    public TableGroup tg1h, tg1d, tg1Short, tg2h, tg3d, tg4h, tg5h, tg6h, tg1All, tgna

    // Logical tables for table groups, na for 'non-aggregatable'
    public LogicalTable lt12, lt13, lt14, lt1All, ltna

    public LogicalTableDictionary logicalDictionary

    public TableIdentifier ti2h, ti2d, ti3d, ti4d, ti1All, tina

    public TemplateDruidQuery simpleTemplateQuery, simpleNestedTemplateQuery, complexTemplateQuery, simpleTemplateWithGrainQuery, complexTemplateWithInnerGrainQuery, complexTemplateWithDoubleGrainQuery

    public DimensionDictionary dimensionDictionary

    public MetricDictionary metricDictionary

    public DataSourceMetadataService metadataService

    public QueryBuildingTestingResources() {
        init()
    }

    public QueryBuildingTestingResources init() {

        DateTimeZone.setDefault(UTC)
        def ages = ["1": "0-10", "2": "11-14", "3": "14-29", "4": "30-40", "5": "41-59", "6": "60+"]

        metadataService = new TestDataSourceMetadataService([:])

        LinkedHashSet<DimensionField> dimensionFields = [BardDimensionField.ID, BardDimensionField.DESC] as LinkedHashSet

        LinkedHashSet<DimensionConfig> lookupDimConfig = new TestLookupDimensions().getDimensionConfigurationsByApiName(SIZE, SHAPE, COLOR)
        LinkedHashSet<DimensionConfig> registeredLookupDimConfig = new TestRegisteredLookupDimensions().getDimensionConfigurationsByApiName(BREED, SPECIES, OTHER);

        d1 = new KeyValueStoreDimension(
                "dim1",
                "dim1",
                dimensionFields,
                MapStoreManager.getInstance("dim1"),
                ScanSearchProviderManager.getInstance("dim1")
        )

        d2 = new KeyValueStoreDimension(
                "dim2",
                "dim2",
                dimensionFields,
                MapStoreManager.getInstance("dim2"),
                ScanSearchProviderManager.getInstance("dim2")
        )

        d2.addAllDimensionRows(ages.collect { BardDimensionField.makeDimensionRow(d2, it.key, it.value) } as Set)

        d3 = new KeyValueStoreDimension(
                "ageBracket",
                "age_bracket",
                dimensionFields,
                MapStoreManager.getInstance("ageBracket"),
                ScanSearchProviderManager.getInstance("ageBracket")
        )

        d3.addAllDimensionRows(ages.collect { BardDimensionField.makeDimensionRow(d3, it.key, it.value) } as Set)

        d4 = new KeyValueStoreDimension(
                "dim4",
                "dim4",
                dimensionFields,
                MapStoreManager.getInstance("dim4"),
                ScanSearchProviderManager.getInstance("dim4")
        )

        d5 = new KeyValueStoreDimension(
                "dim5",
                "dim5",
                dimensionFields,
                MapStoreManager.getInstance("dim5"),
                ScanSearchProviderManager.getInstance("dim5")
        )

        d6 = new KeyValueStoreDimension(
                "dim6",
                "dim6",
                dimensionFields,
                MapStoreManager.getInstance("dim6"),
                ScanSearchProviderManager.getInstance("dim6"),
                false
        )

        d7 = new KeyValueStoreDimension(
                "dim7",
                "dim_7",
                dimensionFields,
                MapStoreManager.getInstance("dim7"),
                ScanSearchProviderManager.getInstance("dim7"),
                false
        )

        // lookup dimensions with multiple, one, and none lookups
        d8 = new LookupDimension(lookupDimConfig.getAt(0))
        d9 = new LookupDimension(lookupDimConfig.getAt(1))
        d10 = new LookupDimension(lookupDimConfig.getAt(2))

        // registered dimensions with multiple, one, and none lookups
        d11 = new RegisteredLookupDimension(registeredLookupDimConfig.getAt(0))
        d12 = new RegisteredLookupDimension(registeredLookupDimConfig.getAt(1))
        d13 = new RegisteredLookupDimension(registeredLookupDimConfig.getAt(2))

        dimensionDictionary = new DimensionDictionary()
        dimensionDictionary.addAll([d1, d2, d3, d4, d5, d6, d7, d8, d9, d10])

        m1 = new LogicalMetric(null, null, "metric1")
        m2 = new LogicalMetric(null, null, "metric2")
        m3 = new LogicalMetric(null, null, "metric3")
        m4 = new LogicalMetric(null, null, "metric4")
        m5 = new LogicalMetric(null, null, "metric5")
        m6 = new LogicalMetric(null, null, "metric6")

        metricDictionary = new MetricDictionary()
        [m1, m2, m3, m4, m5, m6].each {
            metricDictionary.add(it)
        }

        interval1 = new Interval("2014-06-23/2014-07-14")
        interval2 = new Interval("2014-07-07/2014-07-21")
        interval3 = new Interval("2014-07-01/2014-08-01")

        TimeGrain utcHour = HOUR.buildZonedTimeGrain(UTC)
        TimeGrain utcDay = DAY.buildZonedTimeGrain(UTC)

        volatileHourTable = new StrictPhysicalTable(TableName.of("hour"), HOUR.buildZonedTimeGrain(UTC), [d1, m1].collect{toColumn(it)} as Set, [:], metadataService)
        volatileDayTable = new StrictPhysicalTable(TableName.of("day"), DAY.buildZonedTimeGrain(UTC), [d1, m1].collect{toColumn(it)} as Set, [:], metadataService)

        t1h = new StrictPhysicalTable(TableName.of("table1h"), utcHour, [d1, d2, d3, m1, m2, m3].collect{toColumn(it)} as Set, ["ageBracket":"age_bracket"], metadataService)
        t1d = new StrictPhysicalTable(TableName.of("table1d"), utcDay, [d1, d2, d3, m1, m2, m3].collect{toColumn(it)} as Set, ["ageBracket":"age_bracket"], metadataService)
        t1hShort = new StrictPhysicalTable(TableName.of("table1Short"), utcHour, [d1, d2, m1, m2, m3].collect{toColumn(it)} as Set, [:], metadataService)

        t2h = new StrictPhysicalTable(TableName.of("table2"), utcHour, [d1, d2, d4, m1, m4, m5].collect{toColumn(it)} as Set, [:], metadataService)
        t3d = new StrictPhysicalTable(TableName.of("table3"), utcDay, [d1, d2, d5, m6].collect{toColumn(it)} as Set, [:], metadataService)

        tna1236d = new StrictPhysicalTable(TableName.of("tableNA1236"), utcDay, [d1, d2, d3, d6].collect{toColumn(it)} as Set,["ageBracket":"age_bracket"], metadataService)
        tna1237d = new StrictPhysicalTable(TableName.of("tableNA1237"), utcDay, [d1, d2, d3, d7].collect{toColumn(it)} as Set, ["ageBracket":"age_bracket"], metadataService)
        tna167d = new StrictPhysicalTable(TableName.of("tableNA167"), utcDay, [d1, d6, d7].collect{toColumn(it)} as Set, ["ageBracket":"age_bracket", "dim7":"dim_7"], metadataService)
        tna267d = new StrictPhysicalTable(TableName.of("tableNA267"), utcDay, [d2, d6, d7].collect{toColumn(it)} as Set, ["dim7":"dim_7"], metadataService)

        t4h1 = new StrictPhysicalTable(TableName.of("table4h1"), utcHour, [d1, d2, m1, m2, m3].collect{toColumn(it)} as Set, [:], metadataService)
        t4h2 = new StrictPhysicalTable(TableName.of("table4h2"), utcHour, [d1, d2, m1, m2, m3].collect{toColumn(it)} as Set, [:], metadataService)
        t4d1 = new StrictPhysicalTable(TableName.of("table4d1"), utcDay, [d1, d2, m1, m2, m3].collect{toColumn(it)} as Set, [:], metadataService)
        t4d2 = new StrictPhysicalTable(TableName.of("table4d2"), utcDay, [d1, d2, m1, m2, m3].collect{toColumn(it)} as Set, [:], metadataService)

        Map<String, Set<Interval>> availabilityMap1 = [:]
        Map<String, Set<Interval>> availabilityMap2 = [:]

        [d1, d2, m1, m2, m3].each {
            availabilityMap1.put(toColumn(it).getName(), [interval1] as Set)
            availabilityMap2.put(toColumn(it).getName(), [interval2] as Set)
        }

        t4h1.setAvailability(new StrictAvailability(DataSourceName.of(t4h1.name), new TestDataSourceMetadataService(availabilityMap1)))
        t4d1.setAvailability(new StrictAvailability(DataSourceName.of(t4d1.name), new TestDataSourceMetadataService(availabilityMap1)))

        t5h = new StrictPhysicalTable(TableName.of("table5d"), utcHour, [d8, d9, d10, d11, d12, d13, m1].collect{toColumn(it)} as Set, [:], metadataService)

        t4h2.setAvailability(new StrictAvailability(DataSourceName.of(t4h2.name), new TestDataSourceMetadataService(availabilityMap2)))
        t4d2.setAvailability(new StrictAvailability(DataSourceName.of(t4d1.name), new TestDataSourceMetadataService(availabilityMap2)))

        setupPartialData()

        tg1h = new TableGroup([t1h, t1d, t1hShort] as LinkedHashSet, [m1, m2, m3].collect {buildMockName(it.name)} as Set, [d1] as Set)
        tg1d = new TableGroup([t1d] as LinkedHashSet, [m1, m2, m3].collect {buildMockName(it.name)} as Set, [d1] as Set)
        tg1Short = new TableGroup([t1hShort] as LinkedHashSet, [m1, m2, m3].collect {buildMockName(it.name)} as Set, [] as Set)
        tg2h = new TableGroup([t2h] as LinkedHashSet, [m1, m2, m3].collect {buildMockName(it.name)} as Set, [] as Set)
        tg3d = new TableGroup([t3d] as LinkedHashSet, [m1, m2, m3].collect {buildMockName(it.name)} as Set, [] as Set)
        tg4h = new TableGroup([t1h, t2h] as LinkedHashSet, [m1, m2, m3].collect {buildMockName(it.name)} as Set, [] as Set)
        tg5h = new TableGroup([t2h, t1h] as LinkedHashSet, [m1, m2, m3].collect {buildMockName(it.name)} as Set, [] as Set)
        tg6h = new TableGroup([t5h] as LinkedHashSet, [] as Set, [] as Set)
        tgna = new TableGroup([tna1236d, tna1237d, tna167d, tna267d] as LinkedHashSet, [m1, m2, m3].collect {buildMockName(it.name)} as Set, [] as Set)

        lt12 = new LogicalTable("base12", HOUR, tg1h, metricDictionary)
        lt13 = new LogicalTable("base13", DAY, tg1d, metricDictionary)
        lt14 = new LogicalTable("base14", HOUR, tg6h, metricDictionary)
        lt1All = new LogicalTable("baseAll", AllGranularity.INSTANCE, tg1All, metricDictionary)
        ltna = new LogicalTable("baseNA", AllGranularity.INSTANCE, tgna, metricDictionary)

        ti2h = new TableIdentifier("base12", HOUR)
        ti2d = new TableIdentifier("base12", DAY)
        ti3d = new TableIdentifier("base13", DAY)
        ti4d = new TableIdentifier("base14", HOUR)
        tina = new TableIdentifier("baseNA", DAY)

        Map baseMap = [
                (ti2h): lt12,
                (ti2d): lt12,
                (ti3d): lt13,
                (ti4d): lt14,
                (ti1All): lt1All,
                (tina): ltna
        ]

        logicalDictionary = new LogicalTableDictionary()
        logicalDictionary.putAll(baseMap)

        simpleTemplateQuery = new TemplateDruidQuery([] as LinkedHashSet, [] as LinkedHashSet, null, null)
        simpleNestedTemplateQuery = simpleTemplateQuery.nest()
        complexTemplateQuery = new TemplateDruidQuery(
                [] as LinkedHashSet,
                [] as LinkedHashSet,
                simpleTemplateQuery,
                null
        )

        simpleTemplateWithGrainQuery = new TemplateDruidQuery([] as LinkedHashSet, [] as LinkedHashSet, DAY)
        complexTemplateWithInnerGrainQuery = new TemplateDruidQuery(
                [] as LinkedHashSet,
                [] as LinkedHashSet,
                simpleTemplateWithGrainQuery,
                null
        )
        complexTemplateWithDoubleGrainQuery = new TemplateDruidQuery(
                [] as LinkedHashSet,
                [] as LinkedHashSet,
                simpleTemplateWithGrainQuery,
                WEEK
        )

        return this
    }

    def setupPartialData() {
        // In the event of partiality on all data, the coarsest table will be selected and the leftmost of the
        // coarsest tables should be selected
        emptyFirst = new StrictPhysicalTable(TableName.of("emptyFirst"), MONTH.buildZonedTimeGrain(UTC), [d1, m1, m2, m3].collect{toColumn(it)} as Set, [:], metadataService)
        emptyLast = new StrictPhysicalTable(TableName.of("emptyLast"), MONTH.buildZonedTimeGrain(UTC), [d1, m1, m2, m3].collect{toColumn(it)} as Set, [:], metadataService)
        partialSecond = new StrictPhysicalTable(TableName.of("partialSecond"), MONTH.buildZonedTimeGrain(UTC), [d1, m1, m2, m3].collect{toColumn(it)} as Set, [:], metadataService)
        wholeThird = new StrictPhysicalTable(TableName.of("wholeThird"), MONTH.buildZonedTimeGrain(UTC), [d1, m1, m2, m3].collect{toColumn(it)} as Set, [:], metadataService)

        Map<String, Set<Interval>> availabilityMap1 = [:]
        Map<String, Set<Interval>> availabilityMap2 = [:]
        Map<String, Set<Interval>> availabilityMap3 = [:]

        [d1, d2, m1, m2, m3].each {
            availabilityMap1.put(toColumn(it).name, [new Interval("2015/2015")] as Set)
            availabilityMap2.put(toColumn(it).name, [new Interval("2015/2016")] as Set)
            availabilityMap3.put(toColumn(it).name, [new Interval("2011/2016")] as Set)
        }
        emptyFirst.setAvailability(new StrictAvailability(DataSourceName.of(emptyFirst.name), new TestDataSourceMetadataService(availabilityMap1)))
        emptyLast.setAvailability(new StrictAvailability(DataSourceName.of(emptyLast.name), new TestDataSourceMetadataService(availabilityMap1)))
        partialSecond.setAvailability(new StrictAvailability(DataSourceName.of(partialSecond.name), new TestDataSourceMetadataService(availabilityMap2)))
        wholeThird.setAvailability(new StrictAvailability(DataSourceName.of(wholeThird.name), new TestDataSourceMetadataService(availabilityMap3)))

        tg1All = new TableGroup([emptyFirst, partialSecond, wholeThird, emptyLast] as LinkedHashSet, [] as Set, [] as Set)
        ti1All = new TableIdentifier("base1All", AllGranularity.INSTANCE)
    }

    /**
     * Given a collection of triples, each of which contains a physical table, an interval representing the table's
     * availability, and an interval representing the table's volatility, adds the availability information to the
     * table, and initializes the volatileIntervalsService field to map to the correct volatility information.
     *
     * @param physicalTableAvailabilityVolatilityTriples  The collection of triples containing the physical tables,
     * availability, and volatility information
     */
    def setupVolatileTables(Collection<Collection> physicalTableAvailabilityVolatilityTriples) {
        physicalTableAvailabilityVolatilityTriples.each { StrictPhysicalTable table, Interval availability, _ ->
            table.setAvailability(
                    new StrictAvailability(
                            DataSourceName.of(table.name),
                            new TestDataSourceMetadataService(
                                    [new DimensionColumn(d1).name, new LogicalMetricColumn(m1).name].collectEntries() {
                                        [(it): [availability]]
                                    }
                            )
                    )
            )
        }

        volatileIntervalsService = new DefaultingVolatileIntervalsService(
                {[] as SimplifiedIntervalList} as VolatileIntervalsFunction,
                physicalTableAvailabilityVolatilityTriples
                        .collectEntries { PhysicalTable table, _, Interval volatility ->
                                [(table): ({ new SimplifiedIntervalList([volatility]) } as VolatileIntervalsFunction)]
                        }
        )
    }

    ApiMetricName buildMockName(String name) {
        ApiMetricName.of(name)
    }

    Column toColumn(Object item) {
        if (item instanceof Dimension)
            return new DimensionColumn(item)

        if (item instanceof LogicalMetric)
            return new MetricColumn(item.getName())
    }
}
