package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.config.BardFeatureFlag
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.util.JsonSortStrategy

class NoMetricsMultipleLookupDimensionDataServletSpec extends BaseDataServletComponentSpec {

    @Override
    def setup() {
        BardFeatureFlag.REQUIRE_METRICS_QUERY.setOn(false)

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
    def cleanup() {
        BardFeatureFlag.REQUIRE_METRICS_QUERY.reset()
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
                "dateTime": ["2014-05-01%2F2014-05-02"],
        ]
    }

    @Override
    boolean compareResult(String result, String expectedResult, JsonSortStrategy sortStrategy) {
        // Override "test druid query" feature test's compareResult which calls with SORT_BOTH to be default SORT_MAP
        GroovyTestUtils.compareJson(result, expectedResult)
    }

    @Override
    String getExpectedDruidQuery() {
        """{
            "context": {},
            "dataSource": { "name": "all_pets", "type": "table" },
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
                                    "retainMissingValue": false, "type": "lookup"
                                },
                                {
                                    "injective": false,
                                    "lookup": { "namespace": "NAMESPACE2", "type": "namespace" },
                                    "optimize": true,
                                    "replaceMissingValueWith": "Unknown NAMESPACE2",
                                    "retainMissingValue": false, "type": "lookup"
                                }
                        ],
                        "type": "cascade"
                    },
                    "outputName": "breed", "type": "extraction"
                },
                {
                    "dimension": "class",
                    "extractionFn": {
                        "injective": false,
                        "lookup": { "namespace": "NAMESPACE1", "type": "namespace" },
                        "optimize": true,
                        "replaceMissingValueWith": "Unknown NAMESPACE1", "retainMissingValue": false, "type": "lookup"
                    },
                    "outputName": "species",
                    "type": "extraction"
                }
            ],
            "granularity": { "period": "P1D", "timeZone": "UTC", "type": "period" },
            "intervals": ["2014-05-01T00:00:00.000Z/2014-05-02T00:00:00.000Z"],
            "aggregations": [],
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
                  "species" : "species1"
                }
              },
              {
                "version" : "v1",
                "timestamp" : "2014-05-01T00:00:00.000Z",
                "event" : {
                  "sex" : "sex2",
                  "breed" : "breed2",
                  "species" : "species2"
                }
              },
              {
                "version" : "v1",
                "timestamp" : "2014-05-01T00:00:00.000Z",
                "event" : {
                  "sex" : "sex3",
                  "breed" : "breed3",
                  "species" : "species3"
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
                         "sex|desc": "sex1Desc",
                         "sex|id": "sex1",
                         "species|desc": "species1Desc",
                         "species|id": "species1"
                     },
                     {
                         "breed|desc": "breed2Desc",
                         "breed|id": "breed2",
                         "dateTime": "2014-05-01 00:00:00.000",
                         "sex|desc": "sex2Desc",
                         "sex|id": "sex2",
                         "species|desc": "species2Desc",
                         "species|id": "species2"
                     },
                     {
                         "breed|desc": "breed3Desc",
                         "breed|id": "breed3",
                         "dateTime": "2014-05-01 00:00:00.000",
                         "sex|desc": "sex3Desc",
                         "sex|id": "sex3",
                         "species|desc": "species3Desc",
                         "species|id": "species3"
                     }]
        }"""
    }
}
