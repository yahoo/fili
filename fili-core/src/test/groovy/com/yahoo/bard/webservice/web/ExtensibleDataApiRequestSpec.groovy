// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.config.BardFeatureFlag
import com.yahoo.bard.webservice.data.config.ResourceDictionaries
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.metric.LogicalMetricImpl
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import com.yahoo.bard.webservice.data.time.GranularityParser
import com.yahoo.bard.webservice.data.time.StandardGranularityParser
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.LogicalTableDictionary
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.table.TableIdentifier
import com.yahoo.bard.webservice.web.apirequest.ApiRequest
import com.yahoo.bard.webservice.web.apirequest.ExtensibleDataApiRequestImpl

import com.yahoo.bard.webservice.web.apirequest.generator.having.DefaultHavingApiGenerator
import com.yahoo.bard.webservice.web.apirequest.generator.metric.ApiRequestLogicalMetricBinder
import com.yahoo.bard.webservice.web.apirequest.generator.metric.DefaultLogicalMetricGenerator
import com.yahoo.bard.webservice.web.apirequest.generator.orderBy.DefaultOrderByGenerator
import com.yahoo.bard.webservice.web.apirequest.requestParameters.RequestColumn
import com.yahoo.bard.webservice.web.util.BardConfigResources

import org.apache.commons.collections4.MultiValuedMap
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap
import org.joda.time.DateTime

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Subject
import spock.lang.Unroll

import javax.ws.rs.core.PathSegment

class ExtensibleDataApiRequestSpec extends Specification {

    @Shared
    DimensionDictionary dimensionDict
    @Shared
    MetricDictionary metricDict
    @Shared
    LogicalTable table

    @Subject
    ExtensibleDataApiRequestImpl extensibleDataApiRequest

    BardConfigResources bardConfigResources

    def setup() {
        BardFeatureFlag.REQUIRE_METRICS_QUERY.setOn(false)

        LinkedHashSet dimensionFields = [BardDimensionField.ID, BardDimensionField.DESC]
        dimensionDict = new DimensionDictionary()
        KeyValueStoreDimension keyValueStoreDimension
        [ "locale", "one", "two", "three" ].each { String name ->
            keyValueStoreDimension = new KeyValueStoreDimension(name, "druid-"+name, dimensionFields, MapStoreManager.getInstance(name), ScanSearchProviderManager.getInstance(name))
            keyValueStoreDimension.setLastUpdated(new DateTime(10000))
            dimensionDict.add(keyValueStoreDimension)
        }

        metricDict = new MetricDictionary()
        [ "met1", "met2", "met3", "met4" ].each { String name ->
            metricDict.put(name, new LogicalMetricImpl(null, null, name))
        }
        TableGroup tg = Mock(TableGroup)
        tg.getPhysicalTables() >> ([] as Set)
        tg.getApiMetricNames() >> ([] as Set)
        tg.getDimensions() >> dimensionDict.apiNameToDimension.values()
        table = new LogicalTable("name", DAY, tg, metricDict)


        LogicalTableDictionary logicalTableDictionary = new LogicalTableDictionary()
        TableIdentifier tableIdentifier = new TableIdentifier(table.getName(), DAY)
        logicalTableDictionary.put(tableIdentifier, table)
        ResourceDictionaries dictionaries = new ResourceDictionaries(
                null,
                logicalTableDictionary,
                metricDict,
                dimensionDict
        )

        ApiRequestLogicalMetricBinder metricBinder = new DefaultLogicalMetricGenerator()

        GranularityParser granularityParser = new StandardGranularityParser()

        DefaultHavingApiGenerator havingApiGenerator = new DefaultHavingApiGenerator(metricDict);

        DefaultOrderByGenerator orderByGenerator = new DefaultOrderByGenerator()


        bardConfigResources = Mock(BardConfigResources)
        bardConfigResources.getResourceDictionaries() >> dictionaries
        bardConfigResources.getMetricDictionary() >> metricDict
        bardConfigResources.getLogicalTableDictionary() >> logicalTableDictionary
        bardConfigResources.getDimensionDictionary() >> dimensionDict
        bardConfigResources.getGranularityParser() >> granularityParser
        bardConfigResources.getMetricBinder() >> metricBinder
        bardConfigResources.getHavingApiGenerator() >> havingApiGenerator
        bardConfigResources.getOrderByGenerator() >> orderByGenerator
    }

    def cleanup() {
        BardFeatureFlag.REQUIRE_METRICS_QUERY.reset()
    }
    @Unroll
    def "Dataapirequest captures queryParameter under construct and build"() {
        MultiValuedMap<String, String> testParams = new ArrayListValuedHashMap<>()
        testParams.put("testdata", "testValued")

        when:
        extensibleDataApiRequest = new ExtensibleDataApiRequestImpl(
                table.getName(),
                "day", // granularity
                [] List<RequestColumn>, // dimensions
                "", // metrics
                "P1D/Current",
                "", //apiFilters
                "", // havings
                "", // sorts
                null,  // count
                null, // topN
                "json", // format
                "", // filename
                "UTC", // timeZoneId
                -1 as String, //Asynch after
                "", // perPage
                "", // page
                testParams, // queryParams
                null,
                bardConfigResources  // config resources
        )

        then:
        extensibleDataApiRequest.getQueryParameters() == testParams

        when:
        MultiValuedMap<String, String> otherParams = new ArrayListValuedHashMap<>()
        otherParams.put("testdata", "otherTestValue")

        ApiRequest withOtherParams = extensibleDataApiRequest.withQueryParameters(otherParams)

        then:
        otherParams != testParams
        ((ExtensibleDataApiRequestImpl) withOtherParams).queryParameters == otherParams
    }
}
