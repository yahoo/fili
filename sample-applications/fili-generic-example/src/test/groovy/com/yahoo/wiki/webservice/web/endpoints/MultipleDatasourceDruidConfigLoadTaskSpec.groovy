// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.web.endpoints

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.models.druid.client.impl.TestDruidWebService
import com.yahoo.wiki.webservice.data.config.auto.DataSourceConfiguration
import com.yahoo.wiki.webservice.data.config.auto.DruidNavigator

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification

class MultipleDatasourceDruidConfigLoadTaskSpec extends Specification {
    private String[] datasources = ["table1", "table2"]
    private String[] table1_metrics = ["1_metric1", "1_metric2"]
    private String[] table1_dimensions = ["1_dim1", "1_dim2"]
    private String[] table2_metrics = ["2_metric1", "2_metric2"]
    private String[] table2_dimensions = ["2_dim1", "2_dim2"]
    private static final ObjectMapper MAPPER = new ObjectMappersSuite().getMapper()

    private TestDruidWebService druidWebService

    def setup() {
        druidWebService = new TestDruidWebService("testInstance")
        druidWebService.jsonResponse = {
            if (druidWebService.lastUrl.equals("/datasources/")) {
                return MAPPER.writeValueAsString(datasources)
            } else if (druidWebService.lastUrl.equals("/datasources/table1/?full")) {
                return getFullTable(datasources[0], table1_metrics, table1_dimensions)
            } else if (druidWebService.lastUrl.equals("/datasources/table2/?full")) {
                return getFullTable(datasources[1], table2_metrics, table2_dimensions)
            }
        }
    }

    def "Load Multiple datasources"() {
        when: "We query druid"
        DruidNavigator druidNavigator = new DruidNavigator(druidWebService, new ObjectMapper())

        then: "What we expect"
        List<DataSourceConfiguration> tables = druidNavigator.get();
        tables.size() == 2
        DataSourceConfiguration table1 = tables.get(0)
        table1.tableName.asName() == datasources[0]
        table1.metrics.containsAll(table1_metrics.toList())
        table1.dimensions.containsAll(table1_dimensions.toList())

        DataSourceConfiguration table2 = tables.get(1)
        table2.tableName.asName() == datasources[1]
        table2.metrics.containsAll(table2_metrics.toList())
        table2.dimensions.containsAll(table2_dimensions.toList())
    }

    private static String getFullTable(String name, String[] metrics, String[] dimensions) {
        return """{
                    "name": "${name}",
                    "properties": {},
                    "segments": [
                        {
                            "dataSource": "${name}",
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
                            "identifier": "${name}_2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z_2017-02-27T03:06:09.422Z"
                        }
                    ]
                }
                """
    }
}
