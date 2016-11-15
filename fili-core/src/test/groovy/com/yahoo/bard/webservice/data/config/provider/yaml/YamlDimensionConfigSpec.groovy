// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider.yaml

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import spock.lang.Specification

public class YamlDimensionConfigSpec extends Specification {

    static ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    def "Can deserialize from yaml correctly"() {
        setup:
        String conf = """
                     |physical_name: physicalName
                     |long_name: Longer Name
                     |category: some_category
                     |description: some description
                     |fields: [f1, f2]
                     |default_fields: [f1]
                     |aggregatable: false
                     |""".stripMargin()
        def dim = mapper.readValue(conf, YamlDimensionConfig.class)

        expect:
        dim.isAggregatable() == false
        dim.getPhysicalName() == "physicalName"
        dim.getLongName() == "Longer Name"
        dim.getCategory() == "some_category"
        dim.getDescription() == "some description"
        dim.dimensionFieldNames == ["f1", "f2"]
        dim.defaultDimensionFieldNames == ["f1"]
    }

    def "Setting the API name should set missing fields"() {
        setup:
        def dim =  new YamlDimensionConfig(null, null, null, null, null, null, null, null, true)
        dim.setApiName("the_api_name")

        expect:
        dim.getPhysicalName() == "the_api_name"
        dim.getApiName() == "the_api_name"
        dim.getLongName() == "the_api_name"
        dim.getDescription() == "the_api_name"
    }

    def "Duplicate fields should throw an error"() {
        when:
        new YamlDimensionConfig(null, null, null, null, ["f1", "f2", "f2"] as String[], null, null, null, true)

        then:
        RuntimeException ex = thrown()
        ex.message ==~ /.*unique list of dimension fields.*/
    }

    def "Duplicate default fields should throw an error"() {
        when:
        new YamlDimensionConfig(null, null, null, null, null, ["f1", "f2", "f2"] as String[], null, null, true)

        then:
        RuntimeException ex = thrown()
        ex.message ==~ /.*unique list of default dimension fields.*/
    }

    def "Dimension fields works correctly"() {
        setup:

        def dim =  new YamlDimensionConfig(null, null, null, null, ["f1", "f2"] as String[], ["f1"] as String[], null, null, true)

        // Normally called by deserializer
        def availableFields = ["f1": Mock(YamlDimensionFieldConfig), "f2": Mock(YamlDimensionFieldConfig)]
        dim.setAvailableDimensionFields(availableFields)

        expect:
        dim.getDefaultDimensionFields().size() == 1
        dim.getDefaultDimensionFields().contains(availableFields.f1)
        dim.getFields().size() == 2
        dim.getFields().containsAll(availableFields.values())
    }

    def "Dimension fields works correctly to set defaults"() {
        setup:
        def dim =  new YamlDimensionConfig(null, null, null, null, null, null, null, null, true)

        // Normally called by deserializer
        def availableFields = ["f1": Mock(YamlDimensionFieldConfig), "f2": Mock(YamlDimensionFieldConfig), "f3": Mock(YamlDimensionFieldConfig)]
        availableFields.f1.includedByDefault() >> true
        availableFields.f2.includedByDefault() >> false
        availableFields.f3.includedByDefault() >> true
        dim.setAvailableDimensionFields(availableFields)

        expect:
        dim.getDefaultDimensionFields().size() == 2
        dim.getDefaultDimensionFields().containsAll(availableFields.f1, availableFields.f3)
        dim.getFields().size() == 3
        dim.getFields().containsAll(availableFields.values())
    }
}
