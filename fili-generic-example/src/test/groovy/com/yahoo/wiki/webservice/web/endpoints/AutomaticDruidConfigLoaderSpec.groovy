package com.yahoo.wiki.webservice.web.endpoints

import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.data.time.TimeGrain
import com.yahoo.bard.webservice.models.druid.client.impl.TestDruidWebService
import com.yahoo.wiki.webservice.data.config.auto.DataSourceConfiguration
import com.yahoo.wiki.webservice.data.config.auto.DruidNavigator
import com.yahoo.wiki.webservice.data.config.auto.TableConfig

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import spock.lang.Specification
import spock.lang.Unroll

public class AutomaticDruidConfigLoaderSpec extends Specification {
    TestDruidWebService druidWebService;
    DruidNavigator druidNavigator;
    private String[] metrics = ["count", "added", "deleted", "delta", "user_unique"];
    private String[] dimensions = ["channel", "cityName", "comment", "countryIsoCode", "countryName", "isAnonymous",
                                   "isMinor", "isNew", "isRobot", "isUnpatrolled", "metroCode", "namespace", "page",
                                   "regionIsoCode", "regionName", "user"];
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
        druidWebService = new TestDruidWebService("testInstance");
        druidNavigator = new DruidNavigator(druidWebService);
        druidWebService.jsonResponse = {
            if (druidWebService.lastUrl == "/datasources/") {
                return expectedDataSources
            } else if (druidWebService.lastUrl == '/datasources/' + datasource + "/?full") {
                return expectedMetricsAndDimensions
            }
            return "Unexpected URL"
        }
    }

    def "get table names from druid"() {
        setup:

        when: "We send a request"
        List<DataSourceConfiguration> returnedTables = druidNavigator.get();

        then: "what we expect"
        druidWebService.lastUrl == '/datasources/' + datasource + '/?full'
        List<String> returnedTableNames = new ArrayList<>();
        for (DataSourceConfiguration druidConfig : returnedTables) {
            returnedTableNames.add(druidConfig.getName());
        }
        returnedTableNames.contains("wikiticker");
    }

    def "get time grains from druid"() {
        setup:

        when: "We send a request"
        List<DataSourceConfiguration> returnedTables = druidNavigator.get();

        then: "what we expect"
        druidWebService.lastUrl == '/datasources/' + datasource + '/?full'
        returnedTables.get(0).getValidTimeGrains().get(0) == DefaultTimeGrain.DAY
    }

    def "get metric names from druid"() {
        setup:
        TableConfig wikiticker;

        when: "We send a request"
        wikiticker = new TableConfig("$datasource");
        druidNavigator.loadTable(wikiticker);

        then: "what we expect"
        druidWebService.lastUrl == '/datasources/' + datasource + '/?full'
        List<String> returnedMetrics = wikiticker.getMetrics();
        for (String m : metrics) {
            assert returnedMetrics.contains(m);
        }

        List<String> returnedDimensions = wikiticker.getDimensions();
        for (String d : dimensions) {
            assert returnedDimensions.contains(d);
        }
    }

    @Unroll
    def "test getTimeGrain from interval - #expectedTimeGrain"() {
        setup:
        String[] dates = interval.split("/")
        DateTime start = new DateTime(dates[0],DateTimeZone.UTC)
        DateTime end = new DateTime(dates[1],DateTimeZone.UTC)

        when: "we parse the time"
        Optional<TimeGrain> timeGrain = druidNavigator.getTimeGrain(start, end)

        then: "what we expect"
        timeGrain == expectedTimeGrain

        where:
        interval                                            | expectedTimeGrain
        "2015-01-01T00:00:00.000Z/2016-01-01T00:00:00.000Z" | Optional.of(DefaultTimeGrain.YEAR)
        "2017-01-01T00:00:00.000Z/2017-02-01T00:00:00.000Z" | Optional.of(DefaultTimeGrain.MONTH)
        "2017-02-27T00:00:00.000Z/2017-03-06T00:00:00.000Z" | Optional.of(DefaultTimeGrain.WEEK)
        "2015-09-12T00:00:00.000Z/2015-09-13T01:01:01.000Z" | Optional.empty() //expected failure
        "2015-09-12T00:00:00.000Z/2015-09-13T00:00:00.000Z" | Optional.of(DefaultTimeGrain.DAY)
        "2015-09-12T00:00:00.000Z/2015-09-12T01:00:00.000Z" | Optional.of(DefaultTimeGrain.HOUR)
        "2015-09-12T00:00:00.000Z/2015-09-12T00:01:00.000Z" | Optional.of(DefaultTimeGrain.MINUTE)
    }
}