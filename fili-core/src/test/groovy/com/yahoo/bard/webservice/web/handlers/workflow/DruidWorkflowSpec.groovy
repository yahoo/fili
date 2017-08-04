// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers.workflow

import static com.yahoo.bard.webservice.config.BardFeatureFlag.DRUID_CACHE
import static com.yahoo.bard.webservice.config.BardFeatureFlag.DRUID_CACHE_V2
import static com.yahoo.bard.webservice.config.BardFeatureFlag.QUERY_SPLIT

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.config.CacheFeatureFlag
import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.data.PartialDataHandler
import com.yahoo.bard.webservice.data.cache.DataCache
import com.yahoo.bard.webservice.data.cache.TupleDataCache
import com.yahoo.bard.webservice.data.volatility.VolatileIntervalsService
import com.yahoo.bard.webservice.druid.client.DruidWebService
import com.yahoo.bard.webservice.metadata.QuerySigningService
import com.yahoo.bard.webservice.metadata.SegmentIntervalsHashIdGenerator
import com.yahoo.bard.webservice.table.PhysicalTableDictionary
import com.yahoo.bard.webservice.web.handlers.AsyncWebServiceRequestHandler
import com.yahoo.bard.webservice.web.handlers.CacheRequestHandler
import com.yahoo.bard.webservice.web.handlers.CacheV2RequestHandler
import com.yahoo.bard.webservice.web.handlers.DataRequestHandler
import com.yahoo.bard.webservice.web.handlers.DebugRequestHandler
import com.yahoo.bard.webservice.web.handlers.DefaultWebServiceHandlerSelector
import com.yahoo.bard.webservice.web.handlers.DruidPartialDataRequestHandler
import com.yahoo.bard.webservice.web.handlers.EtagCacheRequestHandler
import com.yahoo.bard.webservice.web.handlers.SplitQueryRequestHandler
import com.yahoo.bard.webservice.web.handlers.WebServiceSelectorRequestHandler
import com.yahoo.bard.webservice.web.util.QueryWeightUtil

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification

class DruidWorkflowSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMappersSuite().getMapper()
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance()
    private static final String TTL_CACHE_CONFIG_KEY = SYSTEM_CONFIG.getPackageVariableName("druid_cache_enabled")
    private static final String LOCAL_SIGNATURE_CACHE_CONFIG_KEY = SYSTEM_CONFIG.getPackageVariableName("druid_cache_v2_enabled")
    private static final String ETAG_CACHE_CONFIG_KEY = SYSTEM_CONFIG.getPackageVariableName("query_response_caching_strategy")
    private static final String UNCOVERED_INTERVAL_LIMIT_KEY = SYSTEM_CONFIG.getPackageVariableName("druid_uncovered_interval_limit")

    boolean splittingStatus

    DruidWorkflow dw
    DataCache dataCache = Mock(DataCache)
    DruidWebService webService = Mock(DruidWebService)
    QueryWeightUtil weightUtil = Mock(QueryWeightUtil)
    PhysicalTableDictionary physicalTableDictionary = Mock(PhysicalTableDictionary)
    PartialDataHandler partialDataHandler = Mock(PartialDataHandler)
    QuerySigningService<Long> querySigningService = Mock(SegmentIntervalsHashIdGenerator)
    VolatileIntervalsService volatileIntervalsService = Mock(VolatileIntervalsService)

    String queryResponseCachingStrategy

    def setup() {
        // store config value
        queryResponseCachingStrategy = SYSTEM_CONFIG.getStringProperty(ETAG_CACHE_CONFIG_KEY, "NoCache")

        splittingStatus = QUERY_SPLIT.isOn()
    }

    def cleanup() {
        DRUID_CACHE.reset()
        DRUID_CACHE_V2.reset()

        SYSTEM_CONFIG.clearProperty(TTL_CACHE_CONFIG_KEY)
        SYSTEM_CONFIG.clearProperty(LOCAL_SIGNATURE_CACHE_CONFIG_KEY)
        SYSTEM_CONFIG.clearProperty(ETAG_CACHE_CONFIG_KEY)
        QUERY_SPLIT.setOn(splittingStatus)

        // restore config value
        SYSTEM_CONFIG.setProperty(ETAG_CACHE_CONFIG_KEY, queryResponseCachingStrategy)
    }

    def "Test workflow config controls workflow stages"() {
        setup:
        DRUID_CACHE.setOn(doCache)
        DRUID_CACHE_V2.setOn(doCacheV2)
        CacheFeatureFlag.resetAll()

        when:
        dw = new DruidWorkflow(
                doCacheV2 ? Mock(TupleDataCache) : Mock(DataCache),
                webService,
                weightUtil,
                physicalTableDictionary,
                partialDataHandler,
                querySigningService,
                volatileIntervalsService,
                MAPPER
        )
        DataRequestHandler workflow = dw.buildWorkflow()
        List<DataRequestHandler> handlers = getHandlerChain(workflow)
        WebServiceSelectorRequestHandler select = handlers.find(byClass(WebServiceSelectorRequestHandler))
        def defaultHandler = select.handlerSelector as DefaultWebServiceHandlerSelector

        then:
        defaultHandler.webServiceHandler.getWebService() == webService

        when:
        handlers = getHandlerChain(defaultHandler.webServiceHandler.next)

        then:
        (handlers.find(byClass(CacheRequestHandler)) != null) == isCaching
        (handlers.find(byClass(CacheV2RequestHandler)) != null) == isCachingV2
        handlers.find(byClass(EtagCacheRequestHandler)) == null

        cleanup:
        SYSTEM_CONFIG.clearProperty(TTL_CACHE_CONFIG_KEY)
        SYSTEM_CONFIG.clearProperty(LOCAL_SIGNATURE_CACHE_CONFIG_KEY)

        where:
        doCache | doCacheV2 | isCaching | isCachingV2
        true    | false     | true      | false
        false   | true      | false     | false
        true    | true      | false     | true
        false   | false     | false     | false
    }

    def "Test workflow picks up cache request handler based on query_response_caching_strategy config value when TTL and LocalSig cache are not set"() {
        setup:
        CacheFeatureFlag.resetAll()
        SYSTEM_CONFIG.setProperty(ETAG_CACHE_CONFIG_KEY, etagConfigValue)

        when:
        dw = new DruidWorkflow(
                "LocalSignature".equals(etagConfigValue) || "ETag".equals(etagConfigValue)
                        ? Mock(TupleDataCache)
                        : Mock(DataCache),
                webService,
                weightUtil,
                physicalTableDictionary,
                partialDataHandler,
                querySigningService,
                volatileIntervalsService,
                MAPPER
        )
        DataRequestHandler workflow = dw.buildWorkflow()
        List<DataRequestHandler> handlers = getHandlerChain(workflow)
        WebServiceSelectorRequestHandler select = handlers.find(byClass(WebServiceSelectorRequestHandler))
        def defaultHandler = select.handlerSelector as DefaultWebServiceHandlerSelector

        then:
        defaultHandler.webServiceHandler.getWebService() == webService

        when:
        handlers = getHandlerChain(defaultHandler.webServiceHandler.next)

        then:
        (handlers.find(byClass(CacheRequestHandler)) != null) == isCaching
        (handlers.find(byClass(CacheV2RequestHandler)) != null) == isCachingV2
        (handlers.find(byClass(EtagCacheRequestHandler)) != null) == isEtagCaching

        cleanup:
        SYSTEM_CONFIG.clearProperty(ETAG_CACHE_CONFIG_KEY)

        where:
        etagConfigValue  | isCaching | isCachingV2 | isEtagCaching
        "Ttl"            | true      | false       | false
        "LocalSignature" | false     | true        | false
        "ETag"           | false     | false       | true
        "NoCache"        | false     | false       | false
    }

    def "Test workflow contains standard handlers"() {
        setup:
        dw = new DruidWorkflow(
                dataCache,
                webService,
                weightUtil,
                physicalTableDictionary,
                partialDataHandler,
                querySigningService,
                volatileIntervalsService,
                MAPPER
        )
        DataRequestHandler workflow = dw.buildWorkflow()
        List<DataRequestHandler> handlers = getHandlerChain(workflow)
        WebServiceSelectorRequestHandler select = handlers.find(byClass(WebServiceSelectorRequestHandler))
        def defaultHandler = select.handlerSelector as DefaultWebServiceHandlerSelector

        when:
        handlers = getHandlerChain(defaultHandler.webServiceHandler.next)

        then:
        [AsyncWebServiceRequestHandler, DebugRequestHandler].every {
            handlers.find(byClass(it)) != null
        }
    }

    def "Test workflow contains Splitter when on"() {
        setup:
        QUERY_SPLIT.setOn(true)

        dw = new DruidWorkflow(
                dataCache,
                webService,
                weightUtil,
                physicalTableDictionary,
                partialDataHandler,
                querySigningService,
                volatileIntervalsService,
                MAPPER
        )
        DataRequestHandler workflow = dw.buildWorkflow()
        List<DataRequestHandler> handlers = getHandlerChain(workflow)
        WebServiceSelectorRequestHandler select = handlers.find(byClass(WebServiceSelectorRequestHandler))
        def defaultHandler = select.handlerSelector as DefaultWebServiceHandlerSelector

        when:
        def handlers1 = getHandlerChain(defaultHandler.webServiceHandler.next)

        then:
        handlers1.find(byClass(SplitQueryRequestHandler)) != null

        cleanup:
        QUERY_SPLIT.setOn(splittingStatus)
    }


    List<DataRequestHandler> getHandlerChain(DataRequestHandler fromHandler) {
        def handler = fromHandler
        def result = new ArrayList([handler])
        while (handler.hasProperty("next")) {
            result.add(handler.next)
            handler = handler.next
        }
        result
    }

    def byClass(Class c) {
        { it->it.class == c}
    }

    def "Test workflow contains DruidPartialDataRequestHandler when druidUncoveredIntervalLimit > 0"() {
        setup:
        SYSTEM_CONFIG.setProperty(UNCOVERED_INTERVAL_LIMIT_KEY, '10')
        dw = new DruidWorkflow(
                dataCache,
                webService,
                weightUtil,
                physicalTableDictionary,
                partialDataHandler,
                querySigningService,
                volatileIntervalsService,
                MAPPER
        )
        DataRequestHandler workflow = dw.buildWorkflow()
        List<DataRequestHandler> handlers = getHandlerChain(workflow)
        WebServiceSelectorRequestHandler select = handlers.find(byClass(WebServiceSelectorRequestHandler))
        def defaultHandler = select.handlerSelector as DefaultWebServiceHandlerSelector

        when:
        def handlers1 = getHandlerChain(defaultHandler.webServiceHandler.next)

        then:
        handlers1.find(byClass(DruidPartialDataRequestHandler)) != null

        cleanup:
        SYSTEM_CONFIG.clearProperty(UNCOVERED_INTERVAL_LIMIT_KEY)
    }

    def "Test workflow doesn't contain DruidPartialDataRequestHandler when druidUncoveredIntervalLimit <= 0"() {
        setup:
        SYSTEM_CONFIG.setProperty(UNCOVERED_INTERVAL_LIMIT_KEY, '0')
        dw = new DruidWorkflow(
                dataCache,
                webService,
                weightUtil,
                physicalTableDictionary,
                partialDataHandler,
                querySigningService,
                volatileIntervalsService,
                MAPPER
        )
        DataRequestHandler workflow = dw.buildWorkflow()
        List<DataRequestHandler> handlers = getHandlerChain(workflow)
        WebServiceSelectorRequestHandler select = handlers.find(byClass(WebServiceSelectorRequestHandler))
        def defaultHandler = select.handlerSelector as DefaultWebServiceHandlerSelector

        when:
        def handlers1 = getHandlerChain(defaultHandler.webServiceHandler.next)

        then:
        handlers1.find(byClass(DruidPartialDataRequestHandler)) == null

        cleanup:
        SYSTEM_CONFIG.clearProperty(UNCOVERED_INTERVAL_LIMIT_KEY)
    }
}
