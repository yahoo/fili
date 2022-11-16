// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query

import com.yahoo.bard.webservice.data.config.dimension.TestDimensions
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.MapStore
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.NoOpSearchProvider
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation
import com.yahoo.bard.webservice.druid.model.datasource.DataSource
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.filter.SelectorFilter
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.config.names.DataSourceName
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.table.TableTestUtils
import com.yahoo.bard.webservice.util.GroovyTestUtils

import com.fasterxml.jackson.databind.ObjectMapper

import org.joda.time.DateTimeZone
import org.joda.time.Interval

import spock.lang.Shared
import spock.lang.Specification

import static com.yahoo.bard.webservice.table.TableTestUtils.buildTable

class TimeSeriesQuerySpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMappersSuite().getMapper()

    @Shared
    DateTimeZone currentTZ

    def setupSpec() {
        currentTZ = DateTimeZone.getDefault()
        DateTimeZone.setDefault(DateTimeZone.forTimeZone(TimeZone.getTimeZone("America/Chicago")))
    }

    def shutdownSpec() {
        DateTimeZone.setDefault(currentTZ)
    }

    TimeSeriesQuery defaultQuery(Map vars) {
        vars.dataSource = vars.dataSource ?: new TableDataSource(TableTestUtils.buildTable(
                "table_name",
                DAY.buildZonedTimeGrain(DateTimeZone.UTC),
                [] as Set,
                [:],
                Mock(DataSourceMetadataService) { getAvailableIntervalsByDataSource(_ as DataSourceName) >> [:]}
        ))
        vars.granularity = vars.granularity ?: DAY
        vars.filter = vars.filter ?: null
        vars.having = vars.having ?: null
        vars.aggregations = vars.aggregations ?: new ArrayList<Aggregation>()
        vars.postAggregations = vars.postAggregations ?: new ArrayList<PostAggregation>()
        vars.intervals = vars.intervals ?: new ArrayList<Interval>()
        QueryContext initial = new QueryContext(Collections.<QueryContext.Param, Object> emptyMap(), null)
                .withValue(QueryContext.Param.QUERY_ID, "dummy100")
        QueryContext context = vars.context != null ?
                new QueryContext(initial, vars.context as Map).withValue(QueryContext.Param.QUERY_ID, "dummy100") :
                initial

        new TimeSeriesQuery(
                vars.dataSource,
                vars.granularity,
                vars.filter,
                vars.aggregations,
                vars.postAggregations,
                vars.intervals,
                context,
                false
        )
    }

    def stringQuery(Map vars) {
        vars.queryType = vars.queryType ?: "timeseries"
        vars.dataSource = vars.dataSource ?: '{"type":"table","name":"table_name"}'
        vars.granularity = vars.granularity ?: '{"type":"period","period":"P1D"}'
        vars.filter = vars.filter ? /"filter": $vars.filter,/ : ""
        vars.context = vars.context ?
                /{"queryId":"dummy100",$vars.context}/ :
                /{"queryId": "dummy100"}/
        vars.aggregations = vars.aggregations ?: "[]"
        vars.postAggregations = vars.postAggregations ?: "[]"
        vars.intervals = vars.intervals ?: "[]"


        """
        {
            "queryType":"$vars.queryType",
            "dataSource":$vars.dataSource,
            "granularity": $vars.granularity,
            $vars.filter
            "context":$vars.context,
            "aggregations":$vars.aggregations,
            "postAggregations":$vars.postAggregations,
            "intervals":$vars.intervals
        }
        """
    }

    def "check query serialization"() {
        TimeSeriesQuery dq1 = defaultQuery([:])
        String druidQuery1 = MAPPER.writeValueAsString(dq1)

        String queryString1 = stringQuery([:])

        expect:
        GroovyTestUtils.compareJson(druidQuery1, queryString1)
    }

    def "check with dimensions returns group by query with input dimensions"() {
        ZonedTimeGrain day = DAY.buildZonedTimeGrain(DateTimeZone.UTC)
        DataSource ds1 = new TableDataSource(buildTable("table_name", day, [] as Set, [:], Mock(DataSourceMetadataService) { getAvailableIntervalsByDataSource(_ as DataSourceName) >> [:]} ))
        Dimension locale = Mock(Dimension)
        Filter filter1 = new SelectorFilter(locale, "US")
        Aggregation agg1 = new LongSumAggregation("pageViewsSum", "pageViews")
        Set<Aggregation> aggregation1 = [agg1]
        Set<PostAggregation> postAggregation1 = [new FieldAccessorPostAggregation(agg1)]
        List<Interval> intervals1 = [new Interval("2011-07-04T00:00:00.000Z/2011-07-06T00:00:00.000Z")]
        QueryContext context1 = [(QueryContext.Param.TIMEOUT): 5]
        boolean id = false

        Dimension dimension1 = new KeyValueStoreDimension(
                "a",
                "dim 1",
                new LinkedHashSet<DimensionField>(),
                new MapStore(),
                new NoOpSearchProvider(5)
        )
        Dimension dimension2 = new KeyValueStoreDimension(
                "b",
                "dim 2",
                new LinkedHashSet<DimensionField>(),
                new MapStore(),
                new NoOpSearchProvider(5)
        )
        Collection<Dimension> dimList = new ArrayList<>()
        Collections.addAll(dimList, dimension1, dimension2)
        Collection<Dimension> dimensions = Collections.unmodifiableCollection(dimList)
        TimeSeriesQuery dq1 = new TimeSeriesQuery(ds1, DAY, filter1, aggregation1, postAggregation1, intervals1, context1, id)

        when:
        GroupByQuery dq2 = dq1.withDimensions(dimensions)

        then:
        dq2 instanceof GroupByQuery
        dq2.getDimensions().toArray() == dimensions.toArray()
        (dq2.getDimensions().intersect(dimensions)).size() == 2
        !dq2.getDimensions().disjoint(dimensions)
        dq1.getDataSource() == dq2.getDataSource()
        dq1.getFilter() == dq2.getFilter()
        dq1.getAggregations() == dq2.getAggregations()
        dq1.getPostAggregations() == dq2.getPostAggregations()
        dq1.getIntervals() == dq2.getIntervals()
        dq1.getContext() == dq2.getContext()
    }
}
