// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.web.endpoints

import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.models.druid.client.impl.TestDruidWebService
import com.yahoo.wiki.webservice.data.config.auto.DataSourceConfiguration
import com.yahoo.wiki.webservice.data.config.auto.DruidNavigator
import com.yahoo.wiki.webservice.data.config.auto.TableConfig

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification

class AutomaticDruidConfigLoaderSpec extends Specification {
    TestDruidWebService druidWebService
    DruidNavigator druidNavigator
    private String[] metrics = ["count", "added", "deleted", "delta", "user_unique"]
    private String[] dimensions = ["channel", "cityName", "comment", "countryIsoCode", "countryName", "isAnonymous",
                                   "isMinor", "isNew", "isRobot", "isUnpatrolled", "metroCode", "namespace", "page",
                                   "regionIsoCode", "regionName", "user"]
    String datasource = "wikiticker"
    String expectedDataSources = "[\"$datasource\"]"
    String expectedMetricsAndDimensions = """{
    "name": "wikiticker",
    "properties": {},
    "segments": [
        {
            "dataSource": "wikiticker",
            "interval": "2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.000Z",
            "version": "2017-02-27T03:06:09.422Z",
            "loadSpec":
                {
                    "type": "local",
                    "path": "2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z/2017-02-27T03:06:09.422Z/0/index.zip"
                },
            "dimensions": "${dimensions.join(",")}",
            "metrics":  "${metrics.join(",")}",
            "shardSpec":
                {
                    "type": "none"
                },
            "binaryVersion": 9,
            "size": 5537610,
            "identifier": "wikiticker_2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z_2017-02-27T03:06:09.422Z"
        }
    ]
}
"""

    def setup() {
        druidWebService = new TestDruidWebService("testInstance")
        druidNavigator = new DruidNavigator(druidWebService, new ObjectMapper())
        druidWebService.jsonResponse = {
            if (druidWebService.lastUrl == "/datasources/") {
                return expectedDataSources
            } else if (druidWebService.lastUrl == '/datasources/' + datasource + "/?full") {
                return expectedMetricsAndDimensions
            }
        }
    }

    def "get table names from druid"() {
        when: "We send a request"
        List<DataSourceConfiguration> returnedTables = druidNavigator.get()

        then: "what we expect"
        druidWebService.lastUrl == '/datasources/' + datasource + '/?full'
        List<String> returnedTableNames = new ArrayList<>()
        for (DataSourceConfiguration druidConfig : returnedTables) {
            returnedTableNames.add(druidConfig.getName())
        }
        returnedTableNames.contains("wikiticker")
    }

    def "get time grains from druid"() {
        when: "We send a request"
        List<DataSourceConfiguration> returnedTables = druidNavigator.get()

        then: "what we expect"
        druidWebService.lastUrl == '/datasources/' + datasource + '/?full'
        returnedTables.get(0).getValidTimeGrain() == DefaultTimeGrain.DAY
    }

    def "get metric names from druid"() {
        setup:
        TableConfig wikiticker

        when: "We send a request"
        wikiticker = new TableConfig("$datasource")
        druidNavigator.loadTable(wikiticker)

        then: "what we expect"
        druidWebService.lastUrl == '/datasources/' + datasource + '/?full'
        Set<String> returnedMetrics = wikiticker.getMetrics()
        returnedMetrics.equals(metrics as Set)

        Set<String> returnedDimensions = wikiticker.getDimensions()
        returnedDimensions.equals(dimensions as Set)
    }
}
