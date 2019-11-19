// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.endpoints

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.cache.DataCache
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionRow
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.LuceneSearchProviderManager
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.util.GroovyTestUtils

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import spock.lang.Specification

import javax.ws.rs.client.Entity
import javax.ws.rs.core.Response
import javax.ws.rs.core.Response.Status

class DimensionCacheLoadTaskServletSpec extends Specification {
    private static final ObjectMappersSuite MAPPERS = new ObjectMappersSuite()

    DateTimeZone originalTimeZone

    DimensionCacheLoaderServlet dimensionCacheLoaderServlet

    Dimension dimensionGender
    DimensionRow dimensionRowMale
    DimensionRow dimensionRowFemale
    Set<DimensionRow> dimensionRowsGender

    Dimension dimensionUserCountry
    DimensionRow dimensionRowUSA
    DimensionRow dimensionRowIndia
    Set<DimensionRow> dimensionRowsUserCountry

    LinkedHashSet<DimensionField> dimensionGenderFields
    LinkedHashSet<DimensionField> dimensionUserCountryFields

    DateTime lastUpdated = new DateTime(50000)

    def setup() {
        originalTimeZone = DateTimeZone.getDefault()
        DateTimeZone.setDefault(DateTimeZone.forTimeZone(TimeZone.getTimeZone("America/Chicago")))

        dimensionGenderFields = [BardDimensionField.ID, BardDimensionField.DESC] as LinkedHashSet<DimensionField>
        dimensionUserCountryFields = [
                BardDimensionField.ID,
                BardDimensionField.DESC,
                BardDimensionField.FIELD1,
                BardDimensionField.FIELD2
        ] as LinkedHashSet<DimensionField>

        Set dimensions = [] as Set

        dimensionGender = new KeyValueStoreDimension(
                "gender",
                "gender-description",
                dimensionGenderFields,
                MapStoreManager.getInstance("gender"),
                ScanSearchProviderManager.getInstance("gender")
        )
        dimensionGender.setLastUpdated(lastUpdated)

        dimensionRowMale = BardDimensionField.makeDimensionRow(dimensionGender, "m", "male")
        dimensionRowFemale = BardDimensionField.makeDimensionRow(dimensionGender, "f", "female")
        dimensionRowsGender = [dimensionRowMale, dimensionRowFemale]

        dimensionGender.addAllDimensionRows(dimensionRowsGender)
        dimensions << dimensionGender

        dimensionUserCountry = new KeyValueStoreDimension(
                "user_country",
                "user_country-description",
                dimensionUserCountryFields,
                MapStoreManager.getInstance("user_country"),
                LuceneSearchProviderManager.getInstance("user_country")
        )
        dimensionUserCountry.setLastUpdated(lastUpdated)

        dimensionRowUSA = BardDimensionField.makeDimensionRow(dimensionUserCountry, "usa", "USA", "usa1", "usa2")
        dimensionRowIndia = BardDimensionField.makeDimensionRow(dimensionUserCountry, "ind", "India", "ind1", "ind2")
        dimensionRowsUserCountry = [dimensionRowIndia, dimensionRowUSA]

        dimensionUserCountry.addAllDimensionRows(dimensionRowsUserCountry)
        dimensions << dimensionUserCountry

        DimensionDictionary dimensionDict = new DimensionDictionary(dimensions)
        DataCache dataCache = Mock(DataCache)
        dimensionCacheLoaderServlet = new DimensionCacheLoaderServlet(dimensionDict, dataCache, MAPPERS)
    }

    def cleanup() {
        DateTimeZone.setDefault(originalTimeZone)
        dimensionGender.searchProvider.clearDimension()
        dimensionUserCountry.searchProvider.clearDimension()
    }

    def "UpdateDimensionLastUpdated works as expected"() {
        setup:
        DateTime newLastUpdated = new DateTime(10000)
        String post = """{"name": "gender","lastUpdated":"$newLastUpdated"}"""

        Set<Dimension> dims = dimensionCacheLoaderServlet.dimensionDictionary.findAll()

        when:
        Response r = dimensionCacheLoaderServlet.updateDimensionLastUpdated("gender", post)

        then: "The dimension row we updated has changed"
        dims.find { it.apiName == "gender" }.lastUpdated == newLastUpdated
        r.getStatusInfo() == Status.OK

        and: "Others have not"
        dims.find { it.apiName == "user_country" }.lastUpdated == lastUpdated
    }

    def "UpdateDimensionLastUpdated with unknown dimension gives a NOT FOUND response"() {
        setup:
        String post = """{"name": "unknown","lastUpdated":"${new DateTime(10000)}"}"""

        expect:
        dimensionCacheLoaderServlet.updateDimensionLastUpdated("unknown", post).getStatusInfo() == Status.NOT_FOUND
    }

    def "Check addReplaceDimensionRows for dimension cache loader endpoint"() {
        setup:
        String post = """{
                           "dimensionRows": [
                             {
                               "field1": "foo",
                               "description": "United_States_of_America",
                               "id": "usa"
                             },
                             {
                               "field2": "can2",
                               "description": "Canada",
                               "id": "can"
                             }
                           ]
                         }"""

        Dimension expectedDimensionUserCountry = new KeyValueStoreDimension(
                "user_country",
                "user_country-description",
                dimensionUserCountryFields,
                MapStoreManager.getInstance("expected_user_country"),
                ScanSearchProviderManager.getInstance("expected_user_country")
        )
        expectedDimensionUserCountry.setLastUpdated(lastUpdated)

        Set<DimensionRow> expectedDimensionRowsUserCountry = new HashSet<>()

        expectedDimensionRowsUserCountry << BardDimensionField.makeDimensionRow(
                expectedDimensionUserCountry,
                "usa",
                "United_States_of_America",
                "foo",
                ""
        )
        expectedDimensionRowsUserCountry << dimensionRowIndia
        expectedDimensionRowsUserCountry << BardDimensionField.makeDimensionRow(
                expectedDimensionUserCountry,
                "can",
                "Canada",
                "",
                "can2"
        )

        expectedDimensionUserCountry.addAllDimensionRows(expectedDimensionRowsUserCountry)

        when:
        Response r = dimensionCacheLoaderServlet.addReplaceDimensionRows("user_country", post)

        then:
        r.getStatusInfo() == Status.OK
        expectedDimensionUserCountry.searchProvider.findAllDimensionRows()
                .containsAll(dimensionUserCountry.searchProvider.findAllDimensionRows())
    }

    def "Check addUpdateDimensionRows for dimension cache loader endpoint"() {

        setup:
        String post = """{
                           "dimensionRows": [
                             {
                               "field1": "foo",
                               "description": "United_States_of_America",
                               "id": "usa"
                             },
                             {
                               "field2": "can2",
                               "description": "Canada",
                               "id": "can"
                             }
                           ]
                         }"""

        Dimension expectedDimensionUserCountry = new KeyValueStoreDimension(
                "user_country",
                "user_country-description",
                dimensionUserCountryFields,
                MapStoreManager.getInstance("expected_user_country"),
                ScanSearchProviderManager.getInstance("expected_user_country")
        )
        expectedDimensionUserCountry.setLastUpdated(lastUpdated)

        Set<DimensionRow> expectedDimensionRowsUserCountry = new HashSet<>()

        // USA - description, field1 updated
        expectedDimensionRowsUserCountry << BardDimensionField.makeDimensionRow(
                expectedDimensionUserCountry,
                "usa",
                "United_States_of_America",
                "foo",
                "usa2"
        )
        // India - nothing changes
        expectedDimensionRowsUserCountry << dimensionRowIndia
        // Canada - added row
        expectedDimensionRowsUserCountry << BardDimensionField.makeDimensionRow(
                expectedDimensionUserCountry,
                "can",
                "Canada",
                "",
                "can2"
        )

        expectedDimensionUserCountry.addAllDimensionRows(expectedDimensionRowsUserCountry)

        when:
        Response r = dimensionCacheLoaderServlet.addUpdateDimensionRows("user_country", post)

        then:
        r.getStatusInfo() == Status.OK
        expectedDimensionUserCountry.searchProvider.findAllDimensionRows()
                .containsAll(dimensionUserCountry.searchProvider.findAllDimensionRows())
    }

    def "Check servlet getDimensionLastUpdated"() {
        setup:
        String expected = """{"name":"gender","lastUpdated":"$lastUpdated"}"""

        when:
        Response r = dimensionCacheLoaderServlet.getDimensionLastUpdated("gender")

        then:
        r.getStatusInfo() == Status.OK
        GroovyTestUtils.compareJson(expected, r.getEntity())
    }

    def "Check servlet cache status"() {
        setup:
        String expected = """{"cacheStatus":"Active"}"""

        when:
        Response r = dimensionCacheLoaderServlet.getMsg()

        then:
        r.getStatusInfo() == Status.OK
        GroovyTestUtils.compareJson(expected, r.getEntity())
    }

    def "Delete data cache"() {
        // mock new servlet that validates one call to dataCache clear
        DataCache dataCache2 = Mock(DataCache)
        1 * dataCache2.clear()
        def dimensionCacheLoaderServlet2 = new DimensionCacheLoaderServlet(
                dimensionCacheLoaderServlet.dimensionDictionary,
                dataCache2,
                MAPPERS
        )

        expect:
        dimensionCacheLoaderServlet2.deleteDataCache().getStatusInfo() == Status.OK
    }

    def "Delete data cache endpoint"() {
        setup:
        JerseyTestBinder jtb = new JerseyTestBinder(DimensionCacheLoaderServlet.class)

        expect: "DELETE is allowed"
        jtb.getHarness().target("cache/data").request().delete().getStatusInfo() == Status.OK

        and: "GET is not"
        jtb.getHarness().target("cache/data").request().get().getStatusInfo() == Status.METHOD_NOT_ALLOWED

        cleanup:
        jtb.tearDown()
    }

    def "POST to addReplaceDimensionRows accepts different charsets for application/json"() {
        setup:
        JerseyTestBinder jtb = new JerseyTestBinder(DimensionCacheLoaderServlet.class)
        def postBody = Entity.json("""
                    {
                      "dimensionRows": [
                        {
                          "id": "1",
                          "description": "red"
                        }
                      ]
                    }""")

        expect: "can POST with Content-Type: application/json"
        jtb.getHarness().target("cache/dimensions/color/dimensionRows")
                .request()
                .header("Content-Type", "application/json")
                .post(postBody).getStatusInfo() == Status.OK

        and: "can POST with Content-Type: application/json; charset=utf-8"
        jtb.getHarness().target("cache/dimensions/color/dimensionRows")
                .request()
                .header("Content-Type", "application/json; charset=utf-8")
                .post(postBody).getStatusInfo() == Status.OK

        and: "can POST with Content-Type: application/json; charset=utf-16"
        jtb.getHarness().target("cache/dimensions/color/dimensionRows")
                .request()
                .header("Content-Type", "application/json; charset=utf-16")
                .post(postBody).getStatusInfo() == Status.OK

        cleanup:
        jtb.tearDown()
    }
}
