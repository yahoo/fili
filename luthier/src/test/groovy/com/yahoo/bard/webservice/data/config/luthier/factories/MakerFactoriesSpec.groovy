// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier.factories

import static com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation.ArithmeticPostAggregationFunction.DIVIDE;
import static com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation.ArithmeticPostAggregationFunction.MINUS
import static com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation.ArithmeticPostAggregationFunction.MULTIPLY;
import static com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation.ArithmeticPostAggregationFunction.PLUS;

import com.yahoo.bard.webservice.application.luthier.LuthierConfigNode
import com.yahoo.bard.webservice.application.luthier.LuthierConfigNodeLuaJ
import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.config.luthier.factories.metricmaker.ArithmeticMakerFactory
import com.yahoo.bard.webservice.data.config.metric.makers.AggregationAverageMaker
import com.yahoo.bard.webservice.data.config.metric.makers.LongSumMaker
import com.yahoo.bard.webservice.data.config.luthier.Factory
import com.yahoo.bard.webservice.data.config.luthier.LuthierIndustrialPark
import com.yahoo.bard.webservice.data.config.metric.makers.ArithmeticMaker
import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker

import com.fasterxml.jackson.databind.ObjectMapper

import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.jse.JsePlatform;

import spock.lang.Shared
import spock.lang.Specification

class MakerFactoriesSpec extends Specification {

    LuthierIndustrialPark luthierIndustrialPark = new LuthierIndustrialPark.Builder().build()

    @Shared String plusConfig =
    """
        return {
            arithmetic={
                type="ArithmeticMaker",
                operation="PLUS"
            }
        }
    """

    @Shared String minusConfig = """
        return {
            arithmetic={
                type="ArithmeticMaker",
                operation="MINUS"
            }
        }
    """

    @Shared String multiplyConfig = """
        return {
            arithmetic={
                type="ArithmeticMaker",
                operation="MULTIPLY"
            }
        }
    """

    @Shared String divideConfig = """
        return {
            arithmetic={
                type="ArithmeticMaker",
                operation="DIVIDE"
            }
        }
    """

    Globals globals = JsePlatform.standardGlobals()

    ObjectMapper objectReader = new ObjectMappersSuite().mapper

    def "Test creating factory from Lua table with function #operation"() {
        setup: "Build a factory"
        LuaValue configurationTable = globals.load(config).call()
        LuthierConfigNode arithmeticNode = new LuthierConfigNodeLuaJ(configurationTable);
        Factory<MetricMaker> factory = new ArithmeticMakerFactory()

        when:
        MetricMaker actual = factory.build(
                "arithmetic",
                arithmeticNode.get("arithmetic"),
                luthierIndustrialPark
        )

        then:
        actual instanceof ArithmeticMaker
        actual.function == operation

        where:
        config         | operation
        plusConfig     | PLUS
        minusConfig    | MINUS
        multiplyConfig | MULTIPLY
        divideConfig   | DIVIDE
    }

    def "building longSum metricMaker correctly through a LuthierIndustrialPark"() {
        setup: "Build LuthierIndustrialPark, and then extract the metricMaker"
            MetricMaker longSumMaker = luthierIndustrialPark.getMetricMaker("longSum")
        expect:
            // a pretty "dumb" check that guarantees that there is no exception in build
            // also guarantees that the factoryMap aliases contain "longSum"
            longSumMaker instanceof LongSumMaker
    }

    def "building daily average metricMaker correctly through a LuthierIndustrialPark"() {
        setup: "Build LuthierIndustrialPark, and then extract the metricMaker"
            MetricMaker dailyAvgMaker = luthierIndustrialPark.getMetricMaker("aggregationAverageByDay")
        expect:
            // a pretty "dumb" check that guarantees that there is no exception in build
            // also guarantees that the factoryMap aliases contain "longSum"
            dailyAvgMaker instanceof AggregationAverageMaker
            dailyAvgMaker.innerGrain == luthierIndustrialPark.getGranularityParser().parseGranularity("day")
    }
}
