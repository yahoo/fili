package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.util.JsonSortStrategy

/**
 * Testing Query Time Lookup with extraction filter on Lookup Dimensions.
 */
class LookupDimensionGroupingFilteringDataServletSpec extends BaseDataServletComponentSpec{

    @Override
    def setup() {
        DimensionDictionary dimensionStore = jtb.configurationLoader.dimensionDictionary
        dimensionStore.findByApiName("sex").with {
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "sex1", "sex1Desc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "sex2", "sex2Desc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "sex3", "sex3Desc"))
        }
        dimensionStore.findByApiName("breed").with {
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "breed1", "breed1Desc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "breed2", "breed2Desc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "breed3", "breed3Desc"))
        }
        dimensionStore.findByApiName("species").with {
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "species1", "species1Desc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "species2", "species2Desc"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "species3", "species3Desc"))
        }
    }

    @Override
    boolean compareResult(String result, String expectedResult, JsonSortStrategy sortStrategy) {
        // Override "test druid query" feature test's compareResult which calls with SORT_BOTH to be default SORT_MAP
        GroovyTestUtils.compareJson(result, expectedResult)
    }

    @Override
    Class<?>[] getResourceClasses() {
        [DataServlet.class]
    }

    @Override
    String getTarget() {
        return "data/pets/day/sex/breed/species"
    }

    @Override
    Map<String, List<String>> getQueryParams() {
        [
                "metrics" : ["limbs"],
                "dateTime": ["2014-05-01%2F2014-05-02"],
                "filters" : [
                        "sex|id-in[sex1,sex2]",
                        "breed|desc-in[breed1Desc,breed2Desc]",
                        "species|desc-notin[species3Desc]"

                ]
        ]
    }

    @Override
    String getExpectedDruidQuery() {
        """{
            "aggregations": [
                { "fieldName": "limbs",
                    "name": "limbs",
                    "type": "longSum"
                }
            ],
            "context": {},
            "dataSource": {
                "name": "all_pets",
                "type": "table"
            },
            "dimensions": [
                "sex",
                {
                    "dimension": "breed",
                    "extractionFn": {
                    "extractionFns": [
                            {
                                "injective": false,
                                "lookup": { "namespace": "NAMESPACE1", "type": "namespace" },
                                "optimize": true,
                                "replaceMissingValueWith": "Unknown NAMESPACE1",
                                "retainMissingValue": false,
                                "type": "lookup"
                            },
                            {
                                "injective": false,
                                "lookup": { "namespace": "NAMESPACE2", "type": "namespace" },
                                "optimize": true,
                                "replaceMissingValueWith": "Unknown NAMESPACE2",
                                "retainMissingValue": false,
                                "type": "lookup"
                            }
                    ],
                    "type": "cascade"
                    },
                    "outputName": "breed",
                    "type": "extraction"
                },
                {
                    "dimension": "class",
                    "extractionFn": {
                        "injective": false,
                        "lookup": { "namespace": "NAMESPACE1", "type": "namespace" },
                        "optimize": true,
                        "replaceMissingValueWith": "Unknown NAMESPACE1",
                        "retainMissingValue": false,
                        "type": "lookup"
                    },
                    "outputName": "species",
                    "type": "extraction"
                }
            ],
            "filter": {
                "fields": [
                    {
                        "fields": [
                            { "dimension": "sex",
                                "type": "selector",
                                "value": "sex1"
                            },
                            {
                                "dimension": "sex", "type": "selector", "value": "sex2"
                            }
                        ],
                        "type": "or"
                    },
                    {
                        "fields": [
                            {
                                "dimension": "breed",
                                "extractionFn": {
                                    "extractionFns": [
                                        {
                                            "injective": false,
                                            "lookup":
                                                {
                                                    "namespace": "NAMESPACE1",
                                                    "type": "namespace"
                                                },
                                            "optimize": true,
                                            "replaceMissingValueWith": "Unknown NAMESPACE1",
                                            "retainMissingValue": false,
                                            "type": "lookup"
                                        },
                                        {
                                            "injective": false,
                                            "lookup": {
                                                "namespace": "NAMESPACE2",
                                                "type": "namespace"
                                            },
                                            "optimize": true,
                                            "replaceMissingValueWith": "Unknown NAMESPACE2",
                                            "retainMissingValue": false,
                                            "type": "lookup"
                                        }
                                    ],
                                    "type": "cascade"
                                },
                                "type": "extraction",
                                "value": "breed1"
                            },
                            {
                                "dimension": "breed",
                                "extractionFn": {
                                    "extractionFns": [
                                        {
                                            "injective": false,
                                            "lookup": {
                                                "namespace": "NAMESPACE1",
                                                "type": "namespace"
                                            },
                                            "optimize": true,
                                            "replaceMissingValueWith": "Unknown NAMESPACE1",
                                            "retainMissingValue": false,
                                            "type": "lookup"
                                        },
                                        {
                                            "injective": false,
                                            "lookup": {
                                                "namespace": "NAMESPACE2",
                                                "type": "namespace" },
                                                "optimize": true,
                                                "replaceMissingValueWith": "Unknown NAMESPACE2",
                                                "retainMissingValue": false,
                                                "type": "lookup"
                                        }
                                    ],
                                    "type": "cascade"
                                },
                                "type": "extraction",
                                "value": "breed2"
                            }
                        ], "type": "or"
                    },
                    {
                        "field": {
                            "fields": [
                                {
                                    "dimension": "class",
                                    "extractionFn": {
                                        "injective": false,
                                        "lookup": {
                                            "namespace": "NAMESPACE1",
                                            "type": "namespace"
                                        },
                                        "optimize": true,
                                        "replaceMissingValueWith": "Unknown NAMESPACE1",
                                        "retainMissingValue": false,
                                        "type": "lookup"
                                    },
                                    "type": "extraction",
                                    "value": "species3"
                                }
                            ],
                            "type": "or"
                        },
                        "type": "not"
                    }
                ],
                "type": "and"
            },
            "granularity": {
                "period": "P1D",
                "timeZone": "UTC",
                "type": "period" },
                "intervals": ["2014-05-01T00:00:00.000Z/2014-05-02T00:00:00.000Z"],
                "postAggregations": [],
                "queryType": "groupBy"
        }"""
    }

    @Override
    String getFakeDruidResponse() {
        """[
              {
                "version" : "v1",
                "timestamp" : "2014-05-01T00:00:00.000Z",
                "event" : {
                  "sex" : "sex1",
                  "breed" : "breed1",
                  "species" : "species1",
                  "limbs" : 1
                }
              },
              {
                "version" : "v1",
                "timestamp" : "2014-05-01T00:00:00.000Z",
                "event" : {
                  "sex" : "sex2",
                  "breed" : "breed2",
                  "species" : "species2",
                  "limbs" : 2
                }
              }
            ]"""
    }

    @Override
    String getExpectedApiResponse() {
        """{
            "rows": [{
                         "breed|desc": "breed1Desc",
                         "breed|id": "breed1",
                         "dateTime": "2014-05-01 00:00:00.000",
                         "limbs": 1,
                         "sex|desc": "sex1Desc",
                         "sex|id": "sex1",
                         "species|desc": "species1Desc",
                         "species|id": "species1"
                     },
                     {
                         "breed|desc": "breed2Desc",
                         "breed|id": "breed2",
                         "dateTime": "2014-05-01 00:00:00.000",
                         "limbs": 2,
                         "sex|desc": "sex2Desc",
                         "sex|id": "sex2",
                         "species|desc": "species2Desc",
                         "species|id": "species2"
                     }]
        }"""
    }
}
