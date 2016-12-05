// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.provider.yaml

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigException
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.data.config.metric.makers.ThetaSketchSetOperationMaker
import com.yahoo.bard.webservice.data.config.provider.ConfigurationError
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchSetOperationPostAggFunction
import spock.lang.Specification

public class YamlConfigProviderSpec extends Specification {
    static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance()


    def "test missing setting"() {
        setup:
        SYSTEM_CONFIG.resetProperty(SYSTEM_CONFIG.getPackageVariableName(YamlConfigProvider.CONF_YAML_PATH), null)
        when:
        YamlConfigProvider.build(SYSTEM_CONFIG)
        then:
        SystemConfigException ex = thrown()
    }

    def "test setting that doesn't point to file"() {
        setup:
        SYSTEM_CONFIG.setProperty(SYSTEM_CONFIG.getPackageVariableName(YamlConfigProvider.CONF_YAML_PATH), "/hopefully/not/a/file")
        when:
        YamlConfigProvider.build(SYSTEM_CONFIG)
        then:
        ConfigurationError ex = thrown()
        ex.message =~ /Could not read path.*/
    }

    def "Test loading of config file"() {
        setup:
        String path = YamlConfigProviderSpec.class.getResource("/yaml/exampleConfiguration.yaml").getPath()
        SYSTEM_CONFIG.setProperty(SYSTEM_CONFIG.getPackageVariableName(YamlConfigProvider.CONF_YAML_PATH), path)

        when:
        def conf = YamlConfigProvider.build(SYSTEM_CONFIG)

        then:
        // Metric configs should exist
        conf.getBaseMetrics().containsKey("impressions")
        conf.getDerivedMetrics().containsKey("incremented_impressions")

        // Dimension configs should exist with correct fields
        conf.getDimensionConfig().get("tld").getDefaultDimensionFields().collect({a -> a.getName()}) == ["id"]
        conf.getDimensionConfig().get("tld").getFields().collect({a -> a.getName()}) == ["id", "description"]
        conf.getDimensionConfig().get("platform").getDefaultDimensionFields().collect({a -> a.getName()}) == ["id"]
        conf.getDimensionConfig().get("platform").getFields().collect({a -> a.getName()}) == ["id"]

        // physical table configs should exist
        conf.getPhysicalTableConfig().size() == 2
        conf.getPhysicalTableConfig().get("physical_table_1").getMetrics().collect({a -> a.asName()}) == ["impressions"]
        conf.getPhysicalTableConfig().get("physical_table_2").getMetrics().collect({a -> a.asName()}) == ["impressions"]

        // logical table configs should exist
        conf.getLogicalTableConfig().size() == 1
        conf.getLogicalTableConfig().get("logical_table_1").getMetrics() == ["impressions", "incremented_impressions"] as Set
        conf.getLogicalTableConfig().get("logical_table_1").getPhysicalTables() == ["physical_table_1", "physical_table_2"] as Set
        conf.getLogicalTableConfig().get("logical_table_1").getTimeGrains() == [DefaultTimeGrain.DAY, DefaultTimeGrain.HOUR, DefaultTimeGrain.MINUTE] as Set

        // custom maker configs should exist
        conf.getCustomMakerConfig().get("sketch").getMakerClass() == ThetaSketchSetOperationMaker.class
        conf.getCustomMakerConfig().get("sketch").getArguments()[0] == SketchSetOperationPostAggFunction.NOT

    }
}
