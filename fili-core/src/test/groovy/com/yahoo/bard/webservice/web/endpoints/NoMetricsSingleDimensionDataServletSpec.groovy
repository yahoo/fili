package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.config.BardFeatureFlag
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary

class NoMetricsSingleDimensionDataServletSpec extends BaseDataServletComponentSpec {

    @Override
    def setup() {
        BardFeatureFlag.REQUIRE_METRICS_QUERY.setOn(false)

        DimensionDictionary dimensionStore = jtb.configurationLoader.dimensionDictionary
        dimensionStore.findByApiName("model").with {
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "Model1", "Model1Desc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "Model12", "Model12Desc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "Model123", "Model123Desc"))
        }
    }

    @Override
    def cleanup() {
        BardFeatureFlag.REQUIRE_METRICS_QUERY.reset()
    }

    @Override
    Class<?>[] getResourceClasses() {
        [DataServlet.class]
    }

    @Override
    String getTarget() {
        return "data/shapes/week/model"
    }

    @Override
    Map<String, List<String>> getQueryParams() {
        [
                "dateTime": ["2014-06-02%2F2014-06-09"],
                "filters" : ["other|id-in[other1]", "other|desc-in[other2Desc]"],
        ]
    }

    @Override
    String getExpectedApiResponse() {
        """{
            "rows" : [
                {
                    "dateTime" : "2014-06-02 00:00:00.000",
                    "model|id" : "Model1",
                    "model|desc" : "Model1Desc"
                },
                {
                    "dateTime" : "2014-06-02 00:00:00.000",
                    "model|id" : "Model12",
                    "model|desc" : "Model12Desc"
                },
                {
                    "dateTime" : "2014-06-02 00:00:00.000",
                    "model|id" : "Model123",
                    "model|desc" : "Model123Desc"
                }
            ]
        }"""
    }

    @Override
    String getExpectedDruidQuery() {
        """{
            "queryType": "groupBy",
            "granularity": ${getTimeGrainString("week")},
            "intervals": [ "2014-06-02T00:00:00.000Z/2014-06-09T00:00:00.000Z" ],
            "dataSource" : {
                "name" : "all_shapes",
                "type" : "table"
            },
            "dimensions": [
                "model"
            ],
            "filter": {
                "fields": [
                    {
                        "fields": [
                            {
                                "dimension": "misc",
                                "type": "selector",
                                "value": "other1"
                            }
                        ],
                        "type": "or"
                    },
                    {
                        "fields": [
                            {
                                "dimension": "misc",
                                "type": "selector",
                                "value": "other2Desc"
                            }
                        ],
                        "type": "or"
                    }
                ],
                "type": "and"
            },
            "aggregations": [],
            "postAggregations": [],
            "context": {}
        }"""
    }

    @Override
    String getFakeDruidResponse() {
        """[
            {
                "version" : "v1",
                "timestamp" : "2014-06-02T00:00:00.000Z",
                "event" : {
                    "model" : "Model1"
                }
            },
            {
                "version" : "v1",
                "timestamp" : "2014-06-02T00:00:00.000Z",
                "event" : {
                    "model" : "Model12"
                }
            },
            {
                "version" : "v1",
                "timestamp" : "2014-06-02T00:00:00.000Z",
                "event" : {
                    "model" : "Model123"
                }
            }
        ]"""
    }
}
