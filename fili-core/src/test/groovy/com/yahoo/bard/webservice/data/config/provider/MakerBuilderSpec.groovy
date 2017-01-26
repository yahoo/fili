// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider

import com.yahoo.bard.webservice.data.config.metric.makers.ThetaSketchMaker
import com.yahoo.bard.webservice.data.config.provider.descriptor.MakerDescriptor
import com.yahoo.bard.webservice.data.metric.MetricDictionary
import spock.lang.Specification

class MakerBuilderSpec extends Specification {
    def "should construct default makers without error"(){
        setup:
        MakerBuilder builder = new MakerBuilder(null)

        expect:
        builder.availableMakerConstructors.containsKey("longSum")
    }

    def "Should construct configured makers correctly"(){
        setup:
        MakerDescriptor conf = new MakerDescriptor("thetaMaker", ThetaSketchMaker.class.name, [1] as Object[])
        MakerBuilder builder = new MakerBuilder([conf])

        def maker = builder.build("thetaMaker", new MetricDictionary())

        expect:
        maker instanceof ThetaSketchMaker
        maker.sketchSize == 1
    }

}

