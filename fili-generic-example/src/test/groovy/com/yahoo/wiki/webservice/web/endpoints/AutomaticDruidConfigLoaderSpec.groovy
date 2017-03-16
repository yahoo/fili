package com.yahoo.wiki.webservice.web.endpoints

import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.models.druid.client.impl.TestDruidWebService
import com.yahoo.wiki.webservice.data.config.auto.DataSourceConfiguration
import com.yahoo.wiki.webservice.data.config.auto.DruidNavigator
import com.yahoo.wiki.webservice.data.config.auto.TableConfig

import spock.lang.Specification

public class AutomaticDruidConfigLoaderSpec extends Specification {
    TestDruidWebService druidWebService;
    DruidNavigator druidNavigator;
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
                    "path": "home/khinterlong/Desktop/work/druid-0.9.1.1/var/druid/segments/wikiticker/wikiticker/2015-09-12T00:00:00.000Z_2015-09-13T00:00:00.000Z/2017-02-27T03:06:09.422Z/0/index.zip"
                },
            "dimensions": "channel,cityName,comment,countryIsoCode,countryName,isAnonymous,isMinor,isNew,isRobot,isUnpatrolled,metroCode,namespace,page,regionIsoCode,regionName,user",
            "metrics": "count,added,deleted,delta,user_unique",
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
        druidWebService = new TestDruidWebService("testInstance");
        druidNavigator = new DruidNavigator(druidWebService);
        druidWebService.jsonResponse = {
            if (druidWebService.lastUrl == "http://localhost:8081/druid/coordinator/v1/datasources/") {
                return expectedDataSources
            } else if (druidWebService.lastUrl == "http://localhost:8081/druid/coordinator/v1/datasources/$datasource" +
                    "/?full") {
                return expectedMetricsAndDimensions
            }
            return "BAD ERROR WHAT HAPPENED"
        }
    }

    def "get table names from druid"() {
        setup:

        when: "We send a request"
        List<DataSourceConfiguration> returnedTables = druidNavigator.getTableNames();

        then: "what we expect"
        druidWebService.lastUrl == "http://localhost:8081/druid/coordinator/v1/datasources/$datasource/?full"
        List<String> returnedTableNames = new ArrayList<>();
        for (DataSourceConfiguration druidConfig : returnedTables) {
            returnedTableNames.add(druidConfig.getName());
        }
        returnedTableNames.contains("wikiticker");
    }

    def "get time grains from druid"() {
        setup:

        when: "We send a request"
        List<DataSourceConfiguration> returnedTables = druidNavigator.getTableNames();

        then: "what we expect"
        druidWebService.lastUrl == "http://localhost:8081/druid/coordinator/v1/datasources/$datasource/?full"
        returnedTables.get(0).getValidTimeGrains().get(0) == DefaultTimeGrain.DAY
    }

    def "get metric names from druid"() {
        setup:
        TableConfig wikiticker;
        String[] metrics = ["count", "added", "deleted", "delta", "user_unique"];
        String[] dimensions = ["channel", "cityName", "comment", "countryIsoCode", "countryName", "isAnonymous",
                               "isMinor", "isNew", "isRobot", "isUnpatrolled", "metroCode", "namespace", "page",
                               "regionIsoCode", "regionName", "user"];

        when: "We send a request"
        wikiticker = new TableConfig("$datasource");
        druidNavigator.loadTable(wikiticker);

        then: "what we expect"
        druidWebService.lastUrl == "http://localhost:8081/druid/coordinator/v1/datasources/$datasource/?full"
        List<String> returnedMetrics = wikiticker.getMetrics();
        for (String m : metrics) {
            assert returnedMetrics.contains(m);
        }

        List<String> returnedDimensions = wikiticker.getDimensions();
        for (String d : dimensions) {
            assert returnedDimensions.contains(d);
        }
    }
}