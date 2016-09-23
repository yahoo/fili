// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers.workflow

import static com.yahoo.bard.webservice.config.BardFeatureFlag.DRUID_CACHE
import static com.yahoo.bard.webservice.config.BardFeatureFlag.DRUID_CACHE_V2
import static com.yahoo.bard.webservice.config.BardFeatureFlag.QUERY_SPLIT

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
import com.yahoo.bard.webservice.web.handlers.SplitQueryRequestHandler
import com.yahoo.bard.webservice.web.handlers.WebServiceSelectorRequestHandler
import com.yahoo.bard.webservice.web.handlers.WeightCheckRequestHandler
import com.yahoo.bard.webservice.web.util.QueryWeightUtil

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

import spock.lang.Specification

class DruidWorkflowSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))

    boolean cacheStatus
    boolean cacheV2Status
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

    def setup() {
        cacheStatus = DRUID_CACHE.isOn()
        cacheV2Status = DRUID_CACHE_V2.isOn()
        splittingStatus = QUERY_SPLIT.isOn()
    }

    def cleanup() {
        DRUID_CACHE.setOn(cacheStatus)
        DRUID_CACHE_V2.setOn(cacheV2Status)
        QUERY_SPLIT.setOn(splittingStatus)
    }

    def "Test workflow config controls workflow stages"() {
        setup:
        DRUID_CACHE.setOn(doCache)
        DRUID_CACHE_V2.setOn(doCacheV2)

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
        boolean isCaching = handlers.find(byClass(CacheRequestHandler)) != null |
                handlers.find(byClass(CacheV2RequestHandler)) != null
        boolean isCachingV2 = handlers.find(byClass(CacheV2RequestHandler)) != null

        then:
        isCaching == doCache
        isCachingV2 == doCacheV2

        when:
        handlers = getHandlerChain(defaultHandler.nonUiWebServiceHandler.next)
        isCaching = handlers.find(byClass(CacheRequestHandler)) != null |
                handlers.find(byClass(CacheV2RequestHandler)) != null
        isCachingV2 = handlers.find(byClass(CacheV2RequestHandler)) != null

        then:
        isCaching == doCache
        isCachingV2 == doCacheV2

        where:
        doCache | doCacheV2
        false   | false
        true    | false
        true    | true
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
}
