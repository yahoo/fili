// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider

import com.yahoo.bard.webservice.data.config.metric.makers.ThetaSketchMaker
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import spock.lang.Specification

public class MakerDictionarySpec extends Specification {
    def "should construct default makers without error"(){
        setup:
        MakerDictionary dictionary = MakerDictionary.getDefaultMakers(new MetricDictionary())

        expect:
        dictionary.containsKey("longSum")
    }

    def "Should construct configured makers correctly"(){
        setup:
         def maker = MakerDictionary.buildCustomMaker("thetaMaker", ThetaSketchMaker.getName(), [1] as Object[], new MetricDictionary())

        expect:
        maker instanceof ThetaSketchMaker
    }

    def "Should construct configured makers correctly and include defaults"(){
        setup:
        def makerDict = new MakerDictionary()
        MakerDictionary.loadMetricMakers(new MetricDictionary(), makerDict, ["thetaMaker": new MakerConfiguration() {
            String getClassName() { return ThetaSketchMaker.getName(); }
            Object[] getArguments() { return [1] as Object[]; }
        } ])

        expect:
        makerDict.containsKey("longSum")
        makerDict.containsKey("thetaMaker")
    }

}
