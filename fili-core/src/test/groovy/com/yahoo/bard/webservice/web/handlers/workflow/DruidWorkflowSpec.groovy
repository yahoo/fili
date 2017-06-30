// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers.workflow

import static com.yahoo.bard.webservice.config.BardFeatureFlag.DRUID_CACHE
import static com.yahoo.bard.webservice.config.BardFeatureFlag.DRUID_CACHE_V2
import static com.yahoo.bard.webservice.config.BardFeatureFlag.QUERY_SPLIT
import static com.yahoo.bard.webservice.config.CacheFeatureFlag.ETAG
import static com.yahoo.bard.webservice.config.CacheFeatureFlag.LOCAL_SIGNATURE
import static com.yahoo.bard.webservice.config.CacheFeatureFlag.NONE
import static com.yahoo.bard.webservice.config.CacheFeatureFlag.TTL

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
import com.yahoo.bard.webservice.web.handlers.WeightCheckRequestHandler
import com.yahoo.bard.webservice.web.util.QueryWeightUtil

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

import spock.lang.Specification

import javax.management.StringValueExp

class DruidWorkflowSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance()
    private static final String TTL_CACHE_CONFIG_KEY = SYSTEM_CONFIG.getPackageVariableName("druid_cache_enabled")
    private static final String LOCAL_SIGNATURE_CACHE_CONFIG_KEY = SYSTEM_CONFIG.getPackageVariableName("druid_cache_v2_enabled")
    private static final String ETAG_CACHE_CONFIG_KEY = SYSTEM_CONFIG.getPackageVariableName("query_response_caching_strategy")

    boolean splittingStatus

    DruidWorkflow dw
    DataCache dataCache = Mock(DataCache)
    DruidWebService uiWebService = Mock(DruidWebService)
    DruidWebService nonUiWebService = Mock(DruidWebService)
    QueryWeightUtil weightUtil = Mock(QueryWeightUtil)
    PhysicalTableDictionary physicalTableDictionary = Mock(PhysicalTableDictionary)
    PartialDataHandler partialDataHandler = Mock(PartialDataHandler)
    QuerySigningService<Long> querySigningService = Mock(SegmentIntervalsHashIdGenerator)
    VolatileIntervalsService volatileIntervalsService = Mock(VolatileIntervalsService)

    SystemConfig systemConfig
    String uncoveredKey

    String queryResponseCachingStrategy

    def setup() {
        // store config value
        queryResponseCachingStrategy = SYSTEM_CONFIG.getStringProperty(ETAG_CACHE_CONFIG_KEY, "NoCache")

        splittingStatus = QUERY_SPLIT.isOn()
        systemConfig = SystemConfigProvider.getInstance()
        uncoveredKey = SYSTEM_CONFIG.getPackageVariableName("druid_uncovered_interval_limit")
    }

    def cleanup() {
        SYSTEM_CONFIG.clearProperty(TTL_CACHE_CONFIG_KEY)
        SYSTEM_CONFIG.clearProperty(LOCAL_SIGNATURE_CACHE_CONFIG_KEY)
        SYSTEM_CONFIG.clearProperty(ETAG_CACHE_CONFIG_KEY)
        QUERY_SPLIT.setOn(splittingStatus)

        // restore config value
        SYSTEM_CONFIG.setProperty(ETAG_CACHE_CONFIG_KEY, queryResponseCachingStrategy)
    }

    def "Test workflow config controls workflow stages"() {
        setup:
        SYSTEM_CONFIG.setProperty(TTL_CACHE_CONFIG_KEY, String.valueOf(doCache))
        SYSTEM_CONFIG.setProperty(LOCAL_SIGNATURE_CACHE_CONFIG_KEY, String.valueOf(doCacheV2))

        when:
        dw = new DruidWorkflow(
                doCacheV2 ? Mock(TupleDataCache) : Mock(DataCache),
                uiWebService,
                nonUiWebService,
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
        defaultHandler.uiWebServiceHandler.getWebService() == uiWebService
        defaultHandler.nonUiWebServiceHandler.getWebService() == nonUiWebService

        when:
        handlers = getHandlerChain(defaultHandler.uiWebServiceHandler.next)

        then:
        (handlers.find(byClass(CacheRequestHandler)) != null) == isCaching
        (handlers.find(byClass(CacheV2RequestHandler)) != null) == isCachingV2
        handlers.find(byClass(EtagCacheRequestHandler)) == null

        when:
        handlers = getHandlerChain(defaultHandler.nonUiWebServiceHandler.next)

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
        SYSTEM_CONFIG.setProperty(ETAG_CACHE_CONFIG_KEY, etagConfigValue)

        when:
        dw = new DruidWorkflow(
                "LocalSignature".equals(etagConfigValue) || "ETag".equals(etagConfigValue)
                        ? Mock(TupleDataCache)
                        : Mock(DataCache),
                uiWebService,
                nonUiWebService,
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
        defaultHandler.uiWebServiceHandler.getWebService() == uiWebService
        defaultHandler.nonUiWebServiceHandler.getWebService() == nonUiWebService

        when:
        handlers = getHandlerChain(defaultHandler.uiWebServiceHandler.next)

        then:
        (handlers.find(byClass(CacheRequestHandler)) != null) == isCaching
        (handlers.find(byClass(CacheV2RequestHandler)) != null) == isCachingV2
        (handlers.find(byClass(EtagCacheRequestHandler)) != null) == isEtagCaching

        when:
        handlers = getHandlerChain(defaultHandler.nonUiWebServiceHandler.next)

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
                uiWebService,
                nonUiWebService,
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
        handlers = getHandlerChain(defaultHandler.uiWebServiceHandler.next)

        then:
        [AsyncWebServiceRequestHandler, DebugRequestHandler].every {
            handlers.find(byClass(it)) != null
        }
        when:
        handlers = getHandlerChain(defaultHandler.nonUiWebServiceHandler.next)

        then:
        [AsyncWebServiceRequestHandler, DebugRequestHandler, WeightCheckRequestHandler].every {
            handlers.find(byClass(it)) != null
        }
    }

    def "Test workflow contains Splitter when on"() {
        setup:
        QUERY_SPLIT.setOn(true)

        dw = new DruidWorkflow(
                dataCache,
                uiWebService,
                nonUiWebService,
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
        def handlers1 = getHandlerChain(defaultHandler.uiWebServiceHandler.next)
        def handlers2 = getHandlerChain(defaultHandler.nonUiWebServiceHandler.next)

        then:
        handlers1.find(byClass(SplitQueryRequestHandler)) != null
        handlers2.find(byClass(SplitQueryRequestHandler)) != null

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
        systemConfig.setProperty(uncoveredKey, '10')
        dw = new DruidWorkflow(
                dataCache,
                uiWebService,
                nonUiWebService,
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
        def handlers1 = getHandlerChain(defaultHandler.uiWebServiceHandler.next)
        def handlers2 = getHandlerChain(defaultHandler.nonUiWebServiceHandler.next)

        then:
        handlers1.find(byClass(DruidPartialDataRequestHandler)) != null
        handlers2.find(byClass(DruidPartialDataRequestHandler)) != null

        cleanup:
        systemConfig.clearProperty(uncoveredKey)
    }

    def "Test workflow doesn't contain DruidPartialDataRequestHandler when druidUncoveredIntervalLimit <= 0"() {
        setup:
        SystemConfig systemConfig = SystemConfigProvider.getInstance()
        String uncoveredKey = SYSTEM_CONFIG.getPackageVariableName("druid_uncovered_interval_limit")
        systemConfig.setProperty(uncoveredKey, '0')
        dw = new DruidWorkflow(
                dataCache,
                uiWebService,
                nonUiWebService,
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
        def handlers1 = getHandlerChain(defaultHandler.uiWebServiceHandler.next)
        def handlers2 = getHandlerChain(defaultHandler.nonUiWebServiceHandler.next)

        then:
        handlers1.find(byClass(DruidPartialDataRequestHandler)) == null
        handlers2.find(byClass(DruidPartialDataRequestHandler)) == null

        cleanup:
        systemConfig.clearProperty(uncoveredKey)
    }
}
