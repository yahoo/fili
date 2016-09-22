// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import static com.yahoo.bard.webservice.util.JsonSortStrategy.SORT_BOTH

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.config.BardFeatureFlag
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.util.GroovyTestUtils

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Timeout
import spock.lang.Unroll

import javax.ws.rs.client.Invocation

@Timeout(30)    // Fail test if hangs
class DimensionsServletComponentSpec extends Specification {

    static final int NUM_MODELS_TO_GENERATE = 9

    @Shared JerseyTestBinder jtb
    @Shared JsonSlurper jsonSlurper = new JsonSlurper()
    @Shared boolean originalMetadataCollectionNames

    def setupSpec() {
        originalMetadataCollectionNames = BardFeatureFlag.UPDATED_METADATA_COLLECTION_NAMES.isOn()
        BardFeatureFlag.UPDATED_METADATA_COLLECTION_NAMES.setOn(true)

        // Create the test web container to test the resources
        jtb = new JerseyTestBinder(DimensionsServlet.class)
        DimensionDictionary dimensionStore = jtb.configurationLoader.dimensionDictionary

        dimensionStore.findByApiName("shape").with {
            for (i in 1..35) {
                addDimensionRow(BardDimensionField.makeDimensionRow(it, "shape"+i, "shape"+i+"Desc"))
            }
        }

        dimensionStore.findByApiName("shape").with {
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "a_distinct1", "a_distinct1_row"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "a_distinct2", "a_distinct2_row"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "unique", "unique_row"))
        }

        dimensionStore.findByApiName("model").with {
            for (i in 1..NUM_MODELS_TO_GENERATE) {
                addDimensionRow(BardDimensionField.makeDimensionRow(it, "model"+i, "model"+i+"Desc"))
            }
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "filterValue1", "1or2or3"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "filterValue2", "1or2or3"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "filterValue3", "1or2or3"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "filterValue4", "4or5or6"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "filterValue5", "4or5or6"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "filterValue6", "4or5or6"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "filterValue7", "7or8or9"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "filterValue8", "7or8or9"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "filterValue9", "7or8or9"))
        }

        dimensionStore.findByApiName("model").with {
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "a_distinct1", "a_distinct1_row"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "a_distinct2", "a_distinct2_row"))
            addDimensionRow(BardDimensionField.makeDimensionRow(it, "unique", "unique_row"))
        }
    }

    def cleanupSpec() {
        BardFeatureFlag.UPDATED_METADATA_COLLECTION_NAMES.setOn(originalMetadataCollectionNames)
        jtb.tearDown()
    }

    def "test dimensions endpoint"() {
        setup:
        String expectedResponse = """{
                                        "dimensions":
                                        [
                                            {"category": "General", "name": "color", "longName": "color", "uri": "http://localhost:9998/dimensions/color", "cardinality": 0},
                                            {"category": "General", "name": "shape", "longName": "shape", "uri": "http://localhost:9998/dimensions/shape", "cardinality": 38},
                                            {"category": "General", "name": "size", "longName": "size", "uri": "http://localhost:9998/dimensions/size", "cardinality": 0},
                                            {"category": "General", "name": "model", "longName": "model", "uri": "http://localhost:9998/dimensions/model", "cardinality": 21},
                                            {"category": "General", "name": "other", "longName": "other", "uri": "http://localhost:9998/dimensions/other", "cardinality": 100000},
                                            {"category": "General", "name": "sex", "longName": "sex", "uri": "http://localhost:9998/dimensions/sex", "cardinality": 0},
                                            {"category": "General", "name": "species", "longName": "species", "uri": "http://localhost:9998/dimensions/species", "cardinality": 0},
                                            {"category": "General", "name": "breed", "longName": "breed", "uri": "http://localhost:9998/dimensions/breed", "cardinality": 0}
                                        ]
                                    }"""

        when: "We send a request"
        String result = makeRequest("/dimensions", null).get(String.class)

        then: "The response what we expect"
        GroovyTestUtils.compareJson(result, expectedResponse, SORT_BOTH)
    }

    def "test dimension endpoint"() {

        setup:
        String expectedResponse = """{  "cardinality":100000,
                                        "category": "General",
                                        "description": "other",
                                        "fields": [
                                            {
                                                "description": "Dimension ID",
                                                "name": "id"
                                            }, {
                                                "description": "Dimension Description",
                                                "name": "desc"
                                            }
                                        ],
                                        "longName": "other",
                                        "name": "other",
                                        "tables": [
                                            {
                                                "category": "General",
                                                "longName": "shapes",
                                                "name": "shapes",
                                                "granularity": "day",
                                                "uri": "http://localhost:9998/tables/shapes/day"
                                            }, {
                                                "category": "General",
                                                "longName": "shapes",
                                                "name": "shapes",
                                                "granularity": "all",
                                                "uri": "http://localhost:9998/tables/shapes/all"
                                            }, {
                                                "category": "General",
                                                "longName": "shapes",
                                                "name": "shapes",
                                                "granularity": "week",
                                                "uri": "http://localhost:9998/tables/shapes/week"
                                            }, {
                                                "category": "General",
                                                "longName": "shapes",
                                                "name": "shapes",
                                                "granularity": "month",
                                                "uri": "http://localhost:9998/tables/shapes/month"
                                            }, {
                                                "category": "General",
                                                "longName": "monthly",
                                                "name": "monthly",
                                                "granularity": "day",
                                                "uri": "http://localhost:9998/tables/monthly/day"
                                            }, {
                                                "category": "General",
                                                "longName": "monthly",
                                                "name": "monthly",
                                                "granularity": "all",
                                                "uri": "http://localhost:9998/tables/monthly/all"
                                            }, {
                                                "category": "General",
                                                "longName": "monthly",
                                                "name": "monthly",
                                                "granularity": "week",
                                                "uri": "http://localhost:9998/tables/monthly/week"
                                            }, {
                                                "category": "General",
                                                "longName": "monthly",
                                                "name": "monthly",
                                                "granularity": "month",
                                                "uri": "http://localhost:9998/tables/monthly/month"
                                            }, {
                                                "category": "General",
                                                "longName": "hourly",
                                                "name": "hourly",
                                                "granularity": "hour",
                                                "uri": "http://localhost:9998/tables/hourly/hour"
                                            }, {
                                                "category": "General",
                                                "longName": "hourly",
                                                "name": "hourly",
                                                "granularity": "all",
                                                "uri": "http://localhost:9998/tables/hourly/all"
                                            }, {
                                                "category": "General",
                                                "longName": "hourly",
                                                "name": "hourly",
                                                "granularity": "day",
                                                "uri": "http://localhost:9998/tables/hourly/day"
                                            }, {
                                                "category": "General",
                                                "longName": "hourly",
                                                "name": "hourly",
                                                "granularity": "week",
                                                "uri": "http://localhost:9998/tables/hourly/week"
                                            }, {
                                                "category": "General",
                                                "longName": "hourly",
                                                "name": "hourly",
                                                "granularity": "month",
                                                "uri": "http://localhost:9998/tables/hourly/month"
                                            }, {
                                                "category": "General",
                                                "longName": "hourly_monthly",
                                                "name": "hourly_monthly",
                                                "granularity": "hour",
                                                "uri": "http://localhost:9998/tables/hourly_monthly/hour"
                                            }, {
                                                "category": "General",
                                                "longName": "hourly_monthly",
                                                "name": "hourly_monthly",
                                                "granularity": "all",
                                                "uri": "http://localhost:9998/tables/hourly_monthly/all"
                                            }, {
                                                "category": "General",
                                                "longName": "hourly_monthly",
                                                "name": "hourly_monthly",
                                                "granularity": "day",
                                                "uri": "http://localhost:9998/tables/hourly_monthly/day"
                                            }, {
                                                "category": "General",
                                                "longName": "hourly_monthly",
                                                "name": "hourly_monthly",
                                                "granularity": "week",
                                                "uri": "http://localhost:9998/tables/hourly_monthly/week"
                                            }, {
                                                "category": "General",
                                                "longName": "hourly_monthly",
                                                "name": "hourly_monthly",
                                                "granularity": "month",
                                                "uri": "http://localhost:9998/tables/hourly_monthly/month"
                                            }
                                        ],
                                        "values": "http://localhost:9998/dimensions/other/values"
                                    }"""
        when: "We send a request"
        String result = makeRequest("/dimensions/other", null).get(String.class)

        then: "The response what we expect"
        GroovyTestUtils.compareJson(result, expectedResponse, SORT_BOTH)
    }

    def "test dimension value endpoint JSON response"() {
        setup:
        String expectedResponse = """{
                                         "dimensions" : [
                                                    {
                                                        "id" : "a_distinct1",
                                                        "description" : "a_distinct1_row"
                                                    },
                                                    {
                                                        "id" : "a_distinct2",
                                                        "description" : "a_distinct2_row"
                                                    },
                                                    {
                                                        "id" : "shape1",
                                                        "description" : "shape1Desc"
                                                    }
                                         ],
                                         "meta": {
                                            "pagination": {
                                                "currentPage": 1,
                                                "numberOfResults": 38,
                                                "rowsPerPage": 3,
                                                "paginationLinks": {
                                                    "next": "http://localhost:9998/dimensions/shape/values?perPage=3&page=2",
                                                    "last": "http://localhost:9998/dimensions/shape/values?perPage=3&page=13"
                                                }
                                            }
                                         }
                                     }"""

        Map<String, String> queryParams = [page: 1, perPage: 3]
        when: "We send a request"
        def result = makeRequest("/dimensions/shape/values", queryParams)

        then: "The response is what we expect"
        Map actualJson = jsonSlurper.parseText(result.get(String.class))
        Map expectedJson = jsonSlurper.parseText(expectedResponse)

        String actualNext = actualJson.remove("next")
        String expectedNext = expectedJson.remove("next")
        GroovyTestUtils.compareObjects(actualJson, expectedJson)
        GroovyTestUtils.compareURL(actualNext, expectedNext)

        result.head().getHeaderString("Content-Type") == "application/json; charset=utf-8"
    }

    def "test dimension value endpoint empty response"() {
        setup:
        String expectedResponse = """{
                                         "dimensions" : [],
                                     }"""

        when: "We send a request"
        def result = makeRequest("/dimensions/other/values", null)


        then: "The response what we expect"
        Map actualJson = jsonSlurper.parseText(result.get(String.class))
        Map expectedJson = jsonSlurper.parseText(expectedResponse)

        GroovyTestUtils.compareObjects(actualJson, expectedJson)

        result.head().getHeaderString("Content-Type") == "application/json; charset=utf-8"
    }

    def "test dimension value endpoint CSV response header"() {
        setup:
        Map<String, String> queryParams = [ "page" : 1, "perPage": 3, "format": "CSV"]

        String expectedResponse = """id,description
                                     a_distinct1,a_distinct1_row
                                     a_distinct2,a_distinct2_row
                                     shape1,shape1Desc
                                     """.replaceAll("\\r?\\n?\\s","")

        Map<String, String> expectedLinks = [
                ('"last"'): """<http://localhost:9998/dimensions/shape/values?perPage=3&format=CSV&page=13>""",
                ('"next"'): """<http://localhost:9998/dimensions/shape/values?perPage=3&format=CSV&page=2>"""
        ]

        when: "We send a request"
        def result = makeRequest("/dimensions/shape/values", queryParams)

        then: "The response what we expect"
        result.head().getHeaderString("Content-Disposition") == "attachment; filename=dimensions-shape-values.csv"
        result.head().getHeaderString("Content-Type") == "text/csv; charset=utf-8"
        Map<String, String> actualLinks = GroovyTestUtils.splitHeaderLinks(result.head().getHeaderString("Link"))
        actualLinks.every {name, actualLink -> GroovyTestUtils.compareURL(actualLink, expectedLinks[name])}
        result.get(String.class).replaceAll("\\r?\\n?\\s","") == expectedResponse
    }

    @Unroll
    def "A filter that returns no rows when #explicitlyNotExplicitly paginating should return a valid response"() {
        given: "A filter that will return no rows"
        Map<String, String> queryParams = [
                "filters" : "shape|desc-in[nonexistantDescription]"
        ]
        if (paginating) {
            queryParams["page"] = page
            queryParams["perPage"] = perPage
        }

        and: "An expected response"
        String expectedResponse = """{
                                         "dimensions" : []
                                     }"""

        when: "We send a request"
        String result = makeRequest("/dimensions/shape/values", queryParams).get().readEntity(String)

        then: "We get the expected response"
        GroovyTestUtils.compareJson(result, expectedResponse)

        where:
        paginating | page | perPage | explicitlyNotExplicitly
        true       | "1"  | "5"     | "explicitly"
        false      | ""   | ""      | "not explicitly"
    }

    def "test dimension value endpoint filter-chain"() {
        setup:
        Map<String, String> queryParams = [
                "page" : 1,
                "perPage": 10,
                "filters" : """shape|desc-in[shape1Desc,shape2Desc,shape3Desc,shape4Desc,shape5Desc],
                                 shape|desc-notin[shape1Desc]""".replaceAll("\\r?\\n?\\s","")
        ]

        String expectedResponse = """{
                                         "dimensions" : [
                                                    {
                                                    "id" : "shape2",
                                                    "description" : "shape2Desc"
                                                    },
                                                    {
                                                    "id" : "shape3",
                                                    "description" : "shape3Desc"
                                                    },
                                                    {
                                                    "id" : "shape4",
                                                    "description" : "shape4Desc"
                                                    },
                                                    {
                                                    "id" : "shape5",
                                                    "description" : "shape5Desc"
                                                    }
                                         ]
                                     }"""

        when: "We send a request"
        String result = makeRequest("/dimensions/shape/values", queryParams).get(String.class)

        then:
        GroovyTestUtils.compareJson(result, expectedResponse)
    }

    def "test dimension value endpoint starts with filter"() {
        setup:
        Map<String, String> queryParams = ["filters" : "shape|desc-startswith[a_dist,uniq]"]

        String expectedResponse = """{
                                         "dimensions" : [
                                                    {
                                                    "id" : "a_distinct1",
                                                    "description" : "a_distinct1_row"
                                                    },
                                                    {
                                                    "id" : "a_distinct2",
                                                    "description" : "a_distinct2_row"
                                                    },
                                                    {
                                                    "id" : "unique",
                                                    "description" : "unique_row"
                                                    }
                                         ]
                                     }"""

        when: "We send a request"
        String result = makeRequest("/dimensions/shape/values", queryParams).get(String.class)

        then:
        GroovyTestUtils.compareJson(result, expectedResponse)
    }

    def "test dimension value endpoint contains filter"() {
        setup:
        Map<String, String> queryParams = ["filters" : "shape|desc-contains[istin]"]

        String expectedResponse = """{
                                         "dimensions" : [
                                                    {
                                                    "id" : "a_distinct1",
                                                    "description" : "a_distinct1_row"
                                                    },
                                                    {
                                                    "id" : "a_distinct2",
                                                    "description" : "a_distinct2_row"
                                                    }
                                         ]
                                     }"""

        when: "We send a request"
        String result = makeRequest("/dimensions/shape/values", queryParams).get(String.class)

        then:
        GroovyTestUtils.compareJson(result, expectedResponse)
    }

    def "test dimension value endpoint contains filter with not-in"() {
        setup:
        Map<String, String> queryParams = [
                "filters" : "shape|desc-contains[istin],shape|id-notin[a_distinct1,unique]"
        ]

        String expectedResponse = """{
                                         "dimensions" : [
                                                    {
                                                    "id" : "a_distinct2",
                                                    "description" : "a_distinct2_row"
                                                    }
                                         ]
                                     }"""

        when: "We send a request"
        String result = makeRequest("/dimensions/shape/values", queryParams).get(String.class)

        then:
        GroovyTestUtils.compareJson(result, expectedResponse)
    }

    def "test dimension value endpoint contains filter with different contains filter"() {
        setup:
        Map<String, String> queryParams = ["filters" : "shape|desc-contains[istin],shape|id-contains[2]"]

        String expectedResponse = """{
                                         "dimensions" : [
                                                    {
                                                    "id" : "a_distinct2",
                                                    "description" : "a_distinct2_row"
                                                    }
                                         ]
                                     }"""

        when: "We send a request"
        String result = makeRequest("/dimensions/shape/values", queryParams).get(String.class)

        then:
        GroovyTestUtils.compareJson(result, expectedResponse)
    }

    def "test dimension value endpoint contains filter and in with overlap"() {
        setup:
        Map<String, String> queryParams = [
                "filters" : "shape|desc-contains[istin],shape|id-in[a_distinct1,unique]"
        ]

        String expectedResponse = """{
                                         "dimensions" : [
                                                    {
                                                    "id" : "a_distinct1",
                                                    "description" : "a_distinct1_row"
                                                    }
                                         ]
                                     }"""

        when: "We send a request"
        String result = makeRequest("/dimensions/shape/values", queryParams).get(String.class)

        then:
        GroovyTestUtils.compareJson(result, expectedResponse)
    }

    def "test dimension value endpoint filter-chain on large cardinality dimension"() {
        setup:
        Map<String, String> queryParams = [
                page : 1,
                perPage: 10,
                filters : """model|desc-in[model1Desc,model2Desc,model3Desc],
                                 model|desc-notin[model1Desc]""".replaceAll("\\r?\\n?\\s","")
        ]

        String expectedResponse = """{
                                         "dimensions" : [
                                                    {
                                                    "id" : "model2",
                                                    "description" : "model2Desc"
                                                    },
                                                    {
                                                    "id" : "model3",
                                                    "description" : "model3Desc"
                                                    }
                                         ]
                                     }"""

        when: "We send a request"
        String result = makeRequest("/dimensions/model/values", queryParams).get(String.class)

        then:
        GroovyTestUtils.compareJson(result, expectedResponse)
    }

    def "test dimension value endpoint starts with filter on large cardinality dimension"() {
        setup:
        Map<String, String> queryParams = ["filters" : "model|desc-startswith[a_dist,uniq]"]

        String expectedResponse = """{
                                         "dimensions" : [
                                                    {
                                                    "id" : "a_distinct1",
                                                    "description" : "a_distinct1_row"
                                                    },
                                                    {
                                                    "id" : "a_distinct2",
                                                    "description" : "a_distinct2_row"
                                                    },
                                                    {
                                                    "id" : "unique",
                                                    "description" : "unique_row"
                                                    }
                                         ]
                                     }"""

        when: "We send a request"
        String result = makeRequest("/dimensions/model/values", queryParams).get(String.class)

        then:
        GroovyTestUtils.compareJson(result, expectedResponse)
    }

    def "test dimension value endpoint starts with filter with not in"() {
        setup:
        Map<String, String> queryParams = ["filters" : "model|desc-startswith[a_dis,uniq],model|desc-notin[unique_row]"]

        String expectedResponse = """{
                                         "dimensions" : [
                                                    {
                                                    "id" : "a_distinct1",
                                                    "description" : "a_distinct1_row"
                                                    },
                                                    {
                                                    "id" : "a_distinct2",
                                                    "description" : "a_distinct2_row"
                                                    }
                                         ]
                                     }"""

        when: "We send a request"
        String result = makeRequest("/dimensions/model/values", queryParams).get(String.class)

        then:
        GroovyTestUtils.compareJson(result, expectedResponse)
    }

    def "test dimension value endpoint starts with filter with in"() {
        setup:
        Map<String, String> queryParams = ["filters" : "model|desc-startswith[a_dis,uniq],model|desc-in[unique_row]"]

        String expectedResponse = """{
                                         "dimensions" : [
                                                    {
                                                    "id" : "unique",
                                                    "description" : "unique_row"
                                                    }
                                         ]
                                     }"""

        when: "We send a request"
        String result = makeRequest("/dimensions/model/values", queryParams).get(String.class)

        then:
        GroovyTestUtils.compareJson(result, expectedResponse)
    }

    def "test dimension value endpoint contains filter on large cardinality dimension"() {
        setup:
        Map<String, String> queryParams = ["filters" : "model|desc-contains[istin]"]

        String expectedResponse = """{
                                         "dimensions" : [
                                                    {
                                                    "id" : "a_distinct1",
                                                    "description" : "a_distinct1_row"
                                                    },
                                                    {
                                                    "id" : "a_distinct2",
                                                    "description" : "a_distinct2_row"
                                                    }
                                         ]
                                     }"""

        when: "We send a request"
        String result = makeRequest("/dimensions/model/values", queryParams).get(String.class)

        then:
        GroovyTestUtils.compareJson(result, expectedResponse)
    }

    def "Filtering with 'in' filters on different fields of a dimension trims the result set"() {

        given:
        Map<String, String> filters = [
                filters: "model|id-in[filterValue1,filterValue3,filterValue5],model|desc-in[1or2or3]"
        ]

        and:
        String expectedResponse = buildDimensionResultSet([
                [id: "filterValue1", description: "1or2or3"],
                [id: "filterValue3", description: "1or2or3"]
        ])

        when:
        String result = makeRequest("/dimensions/model/values", filters).get(String.class)

        then:
        GroovyTestUtils.compareJson(result, expectedResponse)
    }

    def "The combined 'in' filters on different fields of a dimension trim the result set to nothing"() {
        given:
        Map<String, String> conjunctedFilters = [
                filters: "model|id-in[filterValue1,filterValue2,filterValue3],model|desc-in[4or5or6]"
        ]

        and:

        String expectedResponse = buildDimensionResultSet([])

        when:
        String result = makeRequest("/dimensions/model/values", conjunctedFilters).get(String.class)

        then:
        GroovyTestUtils.compareJson(result, expectedResponse)
    }

    def "Invoking 'in' and 'not-in' on different fields of the same dimension trim the result set"() {
        given:
        Map<String, String> conjunctedFilters = [
                filters: "model|id-in[filterValue1,filterValue4,filterValue8],model|desc-notin[4or5or6,7or8or9]"
        ]

        String expectedConjunctedResponse = buildDimensionResultSet([[id: "filterValue1", description: "1or2or3"]])

        when:
        String conjunctedResult = makeRequest("/dimensions/model/values", conjunctedFilters).get(String.class)

        then:
        GroovyTestUtils.compareJson(conjunctedResult, expectedConjunctedResponse)
    }

    def "Invoking 'in' and 'not-in' on different fields of the same dimension trim the result set to nothing"() {
        given:
        Map<String, String> conjunctedFilters = [
                filters: "model|id-in[filterValue4,filterValue8],model|desc-notin[4or5or6,7or8or9]"
        ]

        and:
        String expectedConjunctedResponse = buildDimensionResultSet([])

        when:
        String conjunctedResult = makeRequest("/dimensions/model/values", conjunctedFilters).get(String.class)

        then:
        GroovyTestUtils.compareJson(conjunctedResult, expectedConjunctedResponse)
    }

    def "Filters 'in' and 'notin' on one field, and 'in' on another field all trim the result set" () {
        given:
        Map<String, String> filters = [
                filters: "model|id-in[filterValue1,filterValue4,filterValue6],model|id-notin[filterValue4],model|desc-in[4or5or6]"
        ]

        String expectedResponse = buildDimensionResultSet([[id: "filterValue6", description: "4or5or6"]])

        when:
        String result = makeRequest("/dimensions/model/values", filters).get(String.class)

        then:
        GroovyTestUtils.compareJson(result, expectedResponse)
    }

    def "Filters 'in' and 'notin' on one field, and 'notin' on another field all trim the result set" () {
        given:
        Map<String, String> filters = [
                filters: "model|id-in[filterValue1,filterValue2,filterValue4,filterValue5],model|id-notin[filterValue2],model|desc-notin[4or5or6]"
        ]

        String expectedResponse = buildDimensionResultSet([[id: "filterValue1", description: "1or2or3"]])

        when:
        String result = makeRequest("/dimensions/model/values", filters).get(String.class)

        then:
        GroovyTestUtils.compareJson(result, expectedResponse)
    }

    def "Filters 'in' and 'notin' on one field trims the result set" () {
        given:
        Map<String, String> filters = [
                filters: "model|id-in[filterValue1,filterValue2,filterValue4,filterValue5],model|id-notin[filterValue2,filterValue4]"
        ]

        String expectedResponse = buildDimensionResultSet([
                [id: "filterValue1", description: "1or2or3"],
                [id: "filterValue5", description: "4or5or6"]
        ])

        when:
        String result = makeRequest("/dimensions/model/values", filters).get(String.class)

        then:
        GroovyTestUtils.compareJson(result, expectedResponse)
    }

    def "Filters 'in' and 'in' on one field trims the result set" () {
        given:
        Map<String, String> filters = [
                filters: "model|id-in[filterValue1,filterValue2,filterValue5],model|id-in[filterValue2,filterValue4,filterValue5]"
        ]

        String expectedResponse = buildDimensionResultSet([
                [id: "filterValue2", description: "1or2or3"],
                [id: "filterValue5", description: "4or5or6"]
        ])

        when:
        String result = makeRequest("/dimensions/model/values", filters).get(String.class)

        then:
        GroovyTestUtils.compareJson(result, expectedResponse)
    }

    def "Filters 'in' and 'in' on one field trims the result set down to nothing" () {
        given:
        Map<String, String> filters = [
                filters: "model|id-in[filterValue1,filterValue2],model|id-in[filterValue4,filterValue5]"
        ]

        String expectedResponse = buildDimensionResultSet([])

        when:
        String result = makeRequest("/dimensions/model/values", filters).get(String.class)

        then:
        GroovyTestUtils.compareJson(result, expectedResponse)
    }

    def "Filters 'notin' and 'notin' on one field trims the result set" () {
        given:
        Map<String, String> filters = [
                filters: "model|id-notin[filterValue1,filterValue2],model|id-notin[filterValue4,filterValue5]"
        ]

        List<Map<String, String>> jsonObjects = [
                [id: "a_distinct1", description:"a_distinct1_row"],
                [id: "a_distinct2", description: "a_distinct2_row"],
        ]
        jsonObjects.addAll([
                [id: "filterValue3", description: "1or2or3"],
                [id: "filterValue6", description: "4or5or6"],
                [id: "filterValue7", description: "7or8or9"],
                [id: "filterValue8", description: "7or8or9"],
                [id: "filterValue9", description: "7or8or9"],
        ])
        /*
        Creates a list of the form:
        {
            [id: "model1",description: "model1Desc"],
            [id: "model2",description: "model2Desc"],
            ...
            [id: "model9",description: "model9Desc"],
        }
         */
        jsonObjects.addAll(
                (1..NUM_MODELS_TO_GENERATE).collect {
                    [id: "model$it", description: "model${it}Desc"]
                }
        )
        jsonObjects.add([id: "unique", description: "unique_row"])

        String expectedResponse = buildDimensionResultSet(jsonObjects)

        when:
        String result = makeRequest("/dimensions/model/values", filters).get(String.class)

        then:
        GroovyTestUtils.compareJson(result, expectedResponse)
    }

    def "Filters 'notin' and 'notin' on one field trims the result set to nothing" () {
        given:
        List<String> filterClauses = [
                "model|id-notin[filterValue1,filterValue2,filterValue3]",
                "model|id-notin[filterValue4,filterValue5,filterValue6]",
                "model|id-notin[filterValue7,filterValue8,filterValue9]",
        ]
        //filter clause of the form model|id-notin[model1,model2,...model9]
        filterClauses.add(
                "model|id-notin[${(1..NUM_MODELS_TO_GENERATE).collect {i -> "model$i"}.join(",")}]"
        )
        filterClauses.add("model|id-notin[a_distinct1,a_distinct2,unique]")

        Map<String, String> filters = [filters: filterClauses.join(",")]

        String expectedResponse = buildDimensionResultSet([])

        when:
        String result = makeRequest("/dimensions/model/values", filters).get(String.class)

        then:
        GroovyTestUtils.compareJson(result, expectedResponse)
    }


    def "Filters 'notin' and 'in' on two different fields trims the result set" () {
        given:
        Map<String, String> filters = [
                filters:[
                        "model|id-in[filterValue1,filterValue2,filterValue4,filterValue5,filterValue7,filterValue8]",
                        "model|id-notin[filterValue1,filterValue7]",
                        "model|desc-in[1or2or3,7or8or9]",
                        "model|desc-notin[1or2or3]",
                ].join(",")
        ]

        String expectedResponse = buildDimensionResultSet([[id: "filterValue8", description: "7or8or9"]])

        when:
        String result = makeRequest("/dimensions/model/values", filters).get(String.class)

        then:
        GroovyTestUtils.compareJson(result, expectedResponse)
    }

    @Unroll
    def "The order in which the filters #filterStrings are invoked doesn't matter"() {
        given: "The query filters"
        List<Map<String, String>> filters = filterStrings.permutations().collect { [filters: it.join(",")] }

        and: "The expected responses"

        String expectedResponse = buildDimensionResultSet([[id: "filterValue1", description: "1or2or3"]])

        when: "We send a response for each possible ordering of the filters"
        List<String> conjunctedResults = filters. collect {
            makeRequest("/dimensions/model/values", it).get(String.class)
        }

        then: "Every ordering returns the same result"
        conjunctedResults.every { GroovyTestUtils.compareJson(it, expectedResponse) }

        where:
        filterStrings << [[
                                  "model|desc-in[1or2or3]",
                                  "model|id-in[filterValue1,filterValue3,filterValue4]",
                                  "model|id-notin[filterValue3]"
                          ]]
    }

    def "test negative perPage is handled correctly"() {
        setup:
        Map<String, String> queryParams = [page: 1, perPage: -1]

        when: "We send a request"
        def result = makeRequest("/dimensions/model/values", queryParams)

        then: "We get the exptected response"
        result.head().getStatus() == 400
    }

    def "test negative page number is handled correctly"() {
        setup:
        Map<String, String> queryParams = [page: -1, perPage: 3]

        when: "We send a request"
        def result = makeRequest("/dimensions/model/values", queryParams)

        then: "We get the expected response"
        result.head().getStatus() == 400
    }

    Invocation.Builder makeRequest(String target, LinkedHashMap<String, Object> queryParams) {
        // Set target of call
        def httpCall = jtb.getHarness().target(target)

        // Add query params to call
        queryParams.each { String key, Object value ->
            httpCall = httpCall.queryParam(key, value)
        }

        // Make the call
        httpCall.request()
    }

    String buildDimensionResultSet(List<Map<String, String>> objectData) {
        return (/{"dimensions": ${JsonOutput.toJson(objectData)}}/)
    }
}
