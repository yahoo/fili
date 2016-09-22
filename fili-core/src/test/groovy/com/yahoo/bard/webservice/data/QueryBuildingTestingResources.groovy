// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data

import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.COLOR
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.SHAPE
import static com.yahoo.bard.webservice.data.config.names.TestApiDimensionName.SIZE
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static org.joda.time.DateTimeZone.UTC

import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig
import com.yahoo.bard.webservice.data.config.dimension.TestLookupDimensions
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionRow
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.LookupDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.time.TimeGrain
import com.yahoo.bard.webservice.data.volatility.DefaultingVolatileIntervalsService
import com.yahoo.bard.webservice.data.volatility.VolatileIntervalsFunction
import com.yahoo.bard.webservice.data.volatility.VolatileIntervalsService
import com.yahoo.bard.webservice.druid.model.query.AllGranularity
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.LogicalTableDictionary
import com.yahoo.bard.webservice.table.PhysicalTable
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.table.TableIdentifier
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.DateTimeZone
import org.joda.time.Interval

public class QueryBuildingTestingResources {

    // Aggregatable dimensions
    public Dimension d1, d2, d3, d4, d5
    // Non-aggregatable dimensions
    public Dimension d6, d7, d8, d9, d10
    public LogicalMetric m1, m2, m3, m4, m5, m6

    public Interval interval1
    public Interval interval2
    public Interval interval3

    public PhysicalTable emptyFirst, partialSecond, wholeThird, emptyLast
    public PhysicalTable t1h, t1d, t1hShort, t2h, t3d, t4h1, t4h2, t4d1, t4d2, t5h

    //Volatility testing
    public PhysicalTable volatileHourTable = new PhysicalTable("hour", HOUR.buildZonedTimeGrain(UTC), [:])
    public PhysicalTable volatileDayTable = new PhysicalTable("day", DAY.buildZonedTimeGrain(UTC), [:])

    public VolatileIntervalsService volatileIntervalsService

    public TableGroup tg1h, tg1d, tg1Short, tg2h, tg3d, tg4h, tg5h, tg6h, tg1All
    LogicalTable lt12, lt13, lt14, lt1All
    LogicalTableDictionary logicalDictionary
    TableIdentifier ti2h, ti2d, ti3d, ti4d, ti1All

    // Tables with non-aggregatable dimensions
    public PhysicalTable tna1236d, tna1237d, tna167d, tna267d
    public TableGroup tgna
    LogicalTable ltna
    TableIdentifier tina

    TemplateDruidQuery simpleTemplateQuery
    TemplateDruidQuery simpleNestedTemplateQuery
    TemplateDruidQuery complexTemplateQuery
    TemplateDruidQuery simpleTemplateWithGrainQuery
    TemplateDruidQuery complexTemplateWithInnerGrainQuery
    TemplateDruidQuery complexTemplateWithDoubleGrainQuery

    DimensionDictionary dimensionDictionary
    MetricDictionary metricDictionary

    Set<DimensionRow> dimensionRows2
    Set<DimensionRow> dimensionRows3

    public QueryBuildingTestingResources() {

        DateTimeZone.setDefault(UTC)
        interval1 = new Interval("2014-06-23/2014-07-14")
        interval2 = new Interval("2014-07-07/2014-07-21")
        interval3 = new Interval("2014-07-01/2014-08-01")

        def ages = ["1": "0-10", "2": "11-14", "3": "14-29", "4": "30-40", "5": "41-59", "6": "60+"]

        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>()
        dimensionFields.add(BardDimensionField.ID)
        dimensionFields.add(BardDimensionField.DESC)

        LinkedHashSet<DimensionConfig> dimConfig = new TestLookupDimensions().getDimensionConfigurationsByApiName(SIZE, SHAPE, COLOR)

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
        dimensionRows2 = ages.collect() {
            BardDimensionField.makeDimensionRow(d2, it.key, it.value)
        }
        d2.addAllDimensionRows(dimensionRows2)
        d3 = new KeyValueStoreDimension(
                "ageBracket",
                "age_bracket",
                dimensionFields,
                MapStoreManager.getInstance("ageBracket"),
                ScanSearchProviderManager.getInstance("ageBracket")
        )
        dimensionRows3 = ages.collect() {
            BardDimensionField.makeDimensionRow(d3, it.key, it.value)
        }
        d3.addAllDimensionRows(dimensionRows3)
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
                "dim7",
                dimensionFields,
                MapStoreManager.getInstance("dim7"),
                ScanSearchProviderManager.getInstance("dim7"),
                false
        )
        d8 = new LookupDimension(dimConfig.getAt(0))
        d9 = new LookupDimension(dimConfig.getAt(1))
        d10 = new LookupDimension(dimConfig.getAt(2))

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

        TimeGrain utcHour = HOUR.buildZonedTimeGrain(UTC)
        TimeGrain utcDay = DAY.buildZonedTimeGrain(UTC)

        t1h = new PhysicalTable("table1h", utcHour, ["ageBracket":"age_bracket"])
        t1d = new PhysicalTable("table1d", utcDay, ["ageBracket":"age_bracket"])
        t1hShort = new PhysicalTable("table1Short", utcHour, new HashMap<>())

        t2h = new PhysicalTable("table2", utcHour, new HashMap<>())
        t3d = new PhysicalTable("table3", utcDay, new HashMap<>())

        tna1236d = new PhysicalTable("tableNA1236", utcDay, ["ageBracket":"age_bracket"])
        tna1237d = new PhysicalTable("tableNA1237", utcDay, ["ageBracket":"age_bracket"])
        tna167d = new PhysicalTable("tableNA167", utcDay, ["ageBracket":"age_bracket"])
        tna267d = new PhysicalTable("tableNA267", utcDay, new HashMap<>())

        t4h1 = new PhysicalTable("table4h1", utcHour, new HashMap<>())
        t4h2 = new PhysicalTable("table4h2", utcHour, new HashMap<>())
        t4d1 = new PhysicalTable("table4d1", utcDay, new HashMap<>())
        t4d2 = new PhysicalTable("table4d2", utcDay, new HashMap<>())

        t5h = new PhysicalTable("table5d", utcHour, new HashMap<>())

        [d1, d2, d3].each {
            t1h.addColumn(DimensionColumn.addNewDimensionColumn(t1h, it))
            t1d.addColumn(DimensionColumn.addNewDimensionColumn(t1d, it))
        }

        [d1, d2].each {
            t1hShort.addColumn(DimensionColumn.addNewDimensionColumn(t1hShort, it))
        }

        [d1, d2, d4].each {
            t2h.addColumn(DimensionColumn.addNewDimensionColumn(t2h, it))
        }

        [d1, d2, d5].each {
            t3d.addColumn(DimensionColumn.addNewDimensionColumn(t3d, it))
        }

        [d1, d2].each {
            t4h1.addColumn(DimensionColumn.addNewDimensionColumn(t4h1, it), [interval1] as Set)
            t4h2.addColumn(DimensionColumn.addNewDimensionColumn(t4h2, it), [interval2] as Set)
            t4d1.addColumn(DimensionColumn.addNewDimensionColumn(t4d1, it), [interval1] as Set)
            t4d2.addColumn(DimensionColumn.addNewDimensionColumn(t4d2, it), [interval2] as Set)
        }

        [d8, d9, d10].each {
            t5h.addColumn(DimensionColumn.addNewDimensionColumn(t5h, it))
        }

        MetricColumn.addNewMetricColumn(t1h, m1.name)
        MetricColumn.addNewMetricColumn(t1d, m1.name)
        MetricColumn.addNewMetricColumn(t1hShort, m1.name)
        MetricColumn.addNewMetricColumn(t2h, m1.name)
        MetricColumn.addNewMetricColumn(t5h, m1.name)

        [m2, m3].each {
            MetricColumn.addNewMetricColumn(t1h, it.name)
            MetricColumn.addNewMetricColumn(t1d, it.name)
            MetricColumn.addNewMetricColumn(t1hShort, it.name)
        }

        [m4, m5].each {
            MetricColumn.addNewMetricColumn(t2h, it.name)
        }

        MetricColumn.addNewMetricColumn(t3d, m6.name)

        [d1, d2, d3, d6].each {
            tna1236d.addColumn(DimensionColumn.addNewDimensionColumn(tna1236d, it))
        }

        [d1, d2, d3, d7].each {
            tna1237d.addColumn(DimensionColumn.addNewDimensionColumn(tna1237d, it))
        }

        [d1, d6, d7].each {
            tna167d.addColumn(DimensionColumn.addNewDimensionColumn(tna167d, it))
        }

        [d2, d6, d7].each {
            tna267d.addColumn(DimensionColumn.addNewDimensionColumn(tna267d, it))
        }

        [d1, d2].each {
            t4h1.addColumn(DimensionColumn.addNewDimensionColumn(t4h1, it), [interval1] as Set)
            t4h2.addColumn(DimensionColumn.addNewDimensionColumn(t4h2, it), [interval2] as Set)
            t4d1.addColumn(DimensionColumn.addNewDimensionColumn(t4d1, it), [interval1] as Set)
            t4d2.addColumn(DimensionColumn.addNewDimensionColumn(t4d2, it), [interval2] as Set)
        }


        MetricColumn.addNewMetricColumn(t1h, m1.name)
        MetricColumn.addNewMetricColumn(t1d, m1.name)
        MetricColumn.addNewMetricColumn(t1hShort, m1.name)
        MetricColumn.addNewMetricColumn(t2h, m1.name)

        [m2, m3].each {
            MetricColumn.addNewMetricColumn(t1h, it.name)
            MetricColumn.addNewMetricColumn(t1d, it.name)
            MetricColumn.addNewMetricColumn(t1hShort, it.name)
        }

        [m4, m5].each {
            MetricColumn.addNewMetricColumn(t2h, it.name)
        }

        MetricColumn.addNewMetricColumn(t3d, m6.name)

        [m1, m2, m3].each {
            t4h1.addColumn(MetricColumn.addNewMetricColumn(t4h1, it.name), [interval1] as Set)
            t4h2.addColumn(MetricColumn.addNewMetricColumn(t4h2, it.name), [interval2] as Set)
            t4d1.addColumn(MetricColumn.addNewMetricColumn(t4d1, it.name), [interval1] as Set)
            t4d2.addColumn(MetricColumn.addNewMetricColumn(t4d2, it.name), [interval2] as Set)
        }

        [t1h, t1d, t1hShort, t2h, t5h, t3d, t4h1, t4h2, t4d1, t4d2, tna1236d, tna1237d, tna167d, tna267d].each {
            it.commit()
        }

        tg1h = new TableGroup([t1h, t1d, t1hShort] as LinkedHashSet, [m1, m2, m3] as Set)
        tg1d = new TableGroup([t1d] as LinkedHashSet, [m1, m2, m3] as Set)
        tg1Short = new TableGroup([t1hShort] as LinkedHashSet, [m1, m2, m3] as Set)
        tg2h = new TableGroup([t2h] as LinkedHashSet, [m1, m2, m3] as Set)
        tg3d = new TableGroup([t3d] as LinkedHashSet, [m1, m2, m3] as Set)
        tg4h = new TableGroup([t1h, t2h] as LinkedHashSet, [m1, m2, m3] as Set)
        tg5h = new TableGroup([t2h, t1h] as LinkedHashSet, [m1, m2, m3] as Set)
        tg6h = new TableGroup([t5h] as LinkedHashSet, [] as Set)

        lt12 = new LogicalTable("base12", HOUR, tg1h)
        lt13 = new LogicalTable("base13", DAY, tg1d)
        lt14 = new LogicalTable("base14", HOUR, tg6h)
        lt1All = new LogicalTable("baseAll", AllGranularity.INSTANCE, tg1All)

        ti2h = new TableIdentifier("base12", HOUR)
        ti2d = new TableIdentifier("base12", DAY)
        ti3d = new TableIdentifier("base13", DAY)
        ti4d = new TableIdentifier("base14", HOUR)

        // Tables with non-agg dimensions
        tgna = new TableGroup([tna1236d, tna1237d, tna167d, tna267d] as LinkedHashSet, [m1, m2, m3] as Set)
        ltna = new LogicalTable("baseNA", AllGranularity.INSTANCE, tgna)
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

        simpleTemplateQuery = new TemplateDruidQuery(new LinkedHashSet(), new LinkedHashSet(), null, null)
        simpleNestedTemplateQuery = simpleTemplateQuery.nest()
        complexTemplateQuery = new TemplateDruidQuery(
                new LinkedHashSet(),
                new LinkedHashSet(),
                simpleTemplateQuery,
                null
        )

        simpleTemplateWithGrainQuery = new TemplateDruidQuery(new LinkedHashSet(), new LinkedHashSet(), DAY)
        complexTemplateWithInnerGrainQuery = new TemplateDruidQuery(
                new LinkedHashSet(),
                new LinkedHashSet(),
                simpleTemplateWithGrainQuery,
                null
        )
        complexTemplateWithDoubleGrainQuery = new TemplateDruidQuery(
                new LinkedHashSet(),
                new LinkedHashSet(),
                simpleTemplateWithGrainQuery,
                WEEK
        )
        setupPartialData()
    }

    def setupPartialData() {
        // In the event of partiality on all data, the coarsest table will be selected and the leftmost of the
        // coarsest tables should be selected
        emptyFirst = new PhysicalTable("emptyFirst", MONTH.buildZonedTimeGrain(UTC), [:])
        emptyLast = new PhysicalTable("emptyLast", MONTH.buildZonedTimeGrain(UTC), [:])
        partialSecond = new PhysicalTable("partialSecond", MONTH.buildZonedTimeGrain(UTC), [:])
        wholeThird = new PhysicalTable("wholeThird", MONTH.buildZonedTimeGrain(UTC), [:])

        Interval emptyInterval = new Interval("2015/2015");
        emptyFirst.addColumn(DimensionColumn.addNewDimensionColumn(emptyFirst, d1), [emptyInterval] as Set)
        emptyLast.addColumn(DimensionColumn.addNewDimensionColumn(emptyLast, d1), [emptyInterval] as Set)
        Interval oneYear = new Interval("2015/2016");
        partialSecond.addColumn(DimensionColumn.addNewDimensionColumn(partialSecond, d1), [oneYear] as Set)
        Interval fiveYears = new Interval("2011/2016")
        wholeThird.addColumn(DimensionColumn.addNewDimensionColumn(wholeThird, d1), [fiveYears] as Set)

        [m1, m2, m3].each {
            emptyFirst.addColumn(MetricColumn.addNewMetricColumn(emptyFirst, it.name), [emptyInterval] as Set)
            emptyLast.addColumn(MetricColumn.addNewMetricColumn(emptyLast, it.name), [emptyInterval] as Set)
            partialSecond.addColumn(MetricColumn.addNewMetricColumn(partialSecond, it.name), [oneYear] as Set)
            wholeThird.addColumn(MetricColumn.addNewMetricColumn(wholeThird, it.name), [fiveYears] as Set)
        }
        emptyFirst.commit()
        emptyLast.commit()
        partialSecond.commit()
        wholeThird.commit()

        tg1All = new TableGroup([emptyFirst, partialSecond, wholeThird, emptyLast] as LinkedHashSet, [m1, m2, m3] as Set)
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
        physicalTableAvailabilityVolatilityTriples.each { PhysicalTable table, Interval availability, _ ->
            table.addColumn(
                    DimensionColumn.addNewDimensionColumn(table, d1),
                    [availability] as Set
            )
            table.addColumn(
                    MetricColumn.addNewMetricColumn(table, m1.getName()),
                    [availability] as Set
            )
            table.commit()
        }

        volatileIntervalsService = new DefaultingVolatileIntervalsService(
                {[] as SimplifiedIntervalList} as VolatileIntervalsFunction,
                physicalTableAvailabilityVolatilityTriples
                        .collectEntries { PhysicalTable table, _, Interval volatility ->
                                [(table): ({ new SimplifiedIntervalList([volatility]) } as VolatileIntervalsFunction)]
                        }
        )
    }
}
