// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider

import com.yahoo.bard.webservice.data.config.metric.makers.ConstantMaker
import spock.lang.Specification

public class MakerConfigurationSpec extends Specification {

    static class MakerConfigurationImpl implements MakerConfiguration {
        @Override
        String getClassName() {
            return ConstantMaker.class.name
        }

        @Override
        Object[] getArguments() {
            return new Object[0]
        }
    }

    def "MakerConfiguration can construct a new class"() {
        setup:
        def conf = new MakerConfigurationImpl()

        expect:
        conf.getMakerClass() == ConstantMaker.class
    }

}
