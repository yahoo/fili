// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import com.yahoo.bard.webservice.data.DruidQueryBuilder
import com.yahoo.bard.webservice.data.PartialDataHandler
import com.yahoo.bard.webservice.data.QueryBuildingTestingResources
import com.yahoo.bard.webservice.druid.model.DefaultQueryType
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery

import static com.yahoo.bard.webservice.config.BardFeatureFlag.TOP_N
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR
import static org.joda.time.DateTimeZone.UTC


import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricImpl
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper

import com.yahoo.bard.webservice.data.volatility.DefaultingVolatileIntervalsService
import com.yahoo.bard.webservice.druid.model.orderby.LimitSpec
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection
import com.yahoo.bard.webservice.druid.model.orderby.TopNMetric
import com.yahoo.bard.webservice.table.resolver.DefaultPhysicalTableResolver
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest
import com.yahoo.bard.webservice.web.apirequest.generator.filter.FilterBinders
import com.yahoo.bard.webservice.web.filters.ApiFilters

import org.joda.time.DateTime
import org.joda.time.Hours
import org.joda.time.Interval

import spock.lang.Shared
import spock.lang.Specification

class DataApiRequestGetAllGroupingDimsSpec extends Specification {
        @Shared QueryBuildingTestingResources resources
        @Shared DefaultPhysicalTableResolver resolver
        @Shared DruidQueryBuilder builder
        @Shared Map filterSpecs
        @Shared Map<String, ApiFilter> apiFiltersByName
        @Shared boolean topNStatus
        @Shared ApiHaving having

        LimitSpec limitSpec
        TopNMetric topNMetric
        DataApiRequest apiRequest
        LogicalMetric lm1
        static LogicalMetricInfo lmi1 = new LogicalMetricInfo("m1")
        LogicalMetricInfo m1LogicalMetric = new LogicalMetricInfo("lm1")
        List<Interval> intervals
        static FilterBinders filterBinders = FilterBinders.instance

        def staticInitialize() {
            resources = new QueryBuildingTestingResources()
            resolver = new DefaultPhysicalTableResolver(new PartialDataHandler(), new DefaultingVolatileIntervalsService())

            builder = new DruidQueryBuilder(
                    resources.logicalDictionary,
                    resolver,
                    resources.druidFilterBuilder,
                    resources.druidHavingBuilder
            )

            filterSpecs = [
                    abie1234 : "ageBracket|id-eq[1,2,3,4]",
                    abde1129 : "ageBracket|desc-eq[11-14,14-29]",
                    abne56   : "ageBracket|id-notin[5,6]",
                    abdne1429: "ageBracket|desc-notin[14-29]"
            ]

            apiFiltersByName = (filterSpecs.collectEntries {
                [(it.key): filterBinders.generateApiFilter(it.value as String, resources.dimensionDictionary)]
            } ) as Map<String, ApiFilter>

            LinkedHashSet<OrderByColumn> orderByColumns = [new OrderByColumn(lmi1.name, SortDirection.DESC)]
            limitSpec = new LimitSpec(orderByColumns)
            topNMetric = new TopNMetric("m1", SortDirection.DESC)
        }

        def setupSpec() {
            staticInitialize()
            topNStatus = TOP_N.isOn();
            TOP_N.setOn(true)
            having = new ApiHaving("$resources.m1.name-eq[1,2,3]" as String, resources.metricDictionary)
        }

        def cleanupSpec() {
            TOP_N.setOn(topNStatus)
        }

        def setup() {
            intervals = [new Interval(new DateTime("2015"), Hours.ONE)]
        }


        def "Check if getAllGroupingDimensions is called to get all Dims from input request & metric TDQs"() {
            setup:
            apiRequest = Mock(DataApiRequest)
            //Request Dimensions
            apiRequest.getDimensions() >> ([resources.d1] as Set)
            apiRequest.getAllGroupingDimensions() >> ([resources.d1, resources.d2] as Set)

            TemplateDruidQuery tdqTest = new TemplateDruidQuery([] as LinkedHashSet, [] as LinkedHashSet)
            TemplateDruidQuery tdqWithDims = tdqTest.withDimensions([resources.d5] as Collection<Dimension>)
            //Metric Dimensions
            lm1 = new LogicalMetricImpl(tdqWithDims, new NoOpResultSetMapper(), m1LogicalMetric)

            apiRequest.getTable() >> resources.lt12
            apiRequest.getGranularity() >> HOUR.buildZonedTimeGrain(UTC)
            apiRequest.getTimeZone() >> UTC
            ApiFilters apiFilters = new ApiFilters(
                    apiFiltersByName.collectEntries {[(resources.d3): [it.value] as Set]} as Map<Dimension, Set<ApiFilter>>
            )
            //Filter Dimensions
            apiRequest.getApiFilters() >> { apiFilters }
            apiRequest.withFilters(_) >> {
                ApiFilters newFilters ->
                    apiFilters = newFilters
                    apiRequest
            }
            apiRequest.getLogicalMetrics() >> ([lm1] as Set)
            apiRequest.getIntervals() >> intervals
            apiRequest.getTopN() >> Optional.empty()
            apiRequest.getSorts() >> ([] as Set)
            apiRequest.getCount() >> Optional.empty()

            when:
            DruidAggregationQuery<?> dq = builder.buildQuery(apiRequest, resources.simpleTemplateQuery)

            then: //getAllGroupingDimensions is called for every buildQuery call
            dq?.queryType == DefaultQueryType.GROUP_BY
            1 * apiRequest.getAllGroupingDimensions()
        }
}
