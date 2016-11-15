// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider.yaml

import spock.lang.Specification

public class YamlDimensionFieldConfigSpec extends Specification {

    def "Defaults set correctly by name"() {
        setup:
        def dim =  new YamlDimensionFieldConfig(null, false);
        dim.setName("some name")

        expect:
        dim.includedByDefault() == false
        dim.getDescription() == "some name"
        dim.getName() == "some name"
    }
}
