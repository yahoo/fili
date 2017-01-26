// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider

import com.yahoo.bard.webservice.data.config.metric.makers.ConstantMaker
import com.yahoo.bard.webservice.data.config.provider.descriptor.MakerDescriptor

import spock.lang.Specification

class MakerConfigurationSpec extends Specification {

    def "MakerConfiguration can construct class object from name"() {
        setup:
        def conf = new MakerDescriptor("constantMaker", ConstantMaker.class.name, null)

        expect:
        conf.getMakerClass() == ConstantMaker.class
    }

}
