// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl

import static com.yahoo.bard.webservice.data.dimension.BardDimensionField.ID
import static com.yahoo.bard.webservice.data.dimension.BardDimensionField.DESC
import static com.yahoo.bard.webservice.data.dimension.BardDimensionField.FIELD1
import static com.yahoo.bard.webservice.data.dimension.BardDimensionField.FIELD2
import static com.yahoo.bard.webservice.data.dimension.BardDimensionField.makeDimensionRow

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionRow
import com.yahoo.bard.webservice.data.dimension.KeyValueStore
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.SearchProvider
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.web.ApiFilter
import com.yahoo.bard.webservice.web.FilterOperation
import com.yahoo.bard.webservice.web.util.PaginationParameters

import org.joda.time.DateTime

import spock.lang.Specification

/**
 * Specification of behavior that all SearchProviders should share.
 */
abstract class SearchProviderSpec<T extends SearchProvider> extends Specification {

    KeyValueStoreDimension keyValueStoreDimension
    T searchProvider

    DimensionRow dimensionRow1
    DimensionRow dimensionRow2
    DimensionRow dimensionRow2a
    DimensionRow dimensionRow3

    List<DimensionRow> dimensionRows

    LogicalTable animalTable
    DimensionDictionary spaceIdDictionary

    def setupSpec() {
        childSetupSpec()
    }

    def cleanupSpec() {
        childCleanupSpec()
    }

    def setup() {
        KeyValueStore keyValueStore = MapStoreManager.getInstance("animal")
        searchProvider = getSearchProvider("animal")

        LinkedHashSet<DimensionField> dimensionFields = [ID, DESC]

        DateTime lastUpdated = new DateTime(10000)
        keyValueStoreDimension = new KeyValueStoreDimension(
                "animal",
                "animal-description",
                dimensionFields,
                keyValueStore,
                searchProvider
        )
        keyValueStoreDimension.setLastUpdated(lastUpdated)

        dimensionRow1 = makeDimensionRow(keyValueStoreDimension, "owl", "this is an owl")
        dimensionRow2 = makeDimensionRow(keyValueStoreDimension, "hawk", "this is a raptor")
        dimensionRow2a = makeDimensionRow(keyValueStoreDimension, "eagle", "this is a raptor")
        dimensionRow3 = makeDimensionRow(keyValueStoreDimension, "kumquat", "this is not an animal")

        dimensionRows = [
                dimensionRow1,
                dimensionRow2,
                dimensionRow2a,
                dimensionRow3,
                makeDimensionRow(keyValueStoreDimension, "chimpanzee", "Monkeys have teeth"),
                makeDimensionRow(keyValueStoreDimension, "bonobo", "Monkeys have teeth"),
                makeDimensionRow(keyValueStoreDimension, "spidermonkey", "Monkeys have teeth"),
                makeDimensionRow(keyValueStoreDimension, "brownrecluse", "Spiders have eight legs"),
                makeDimensionRow(keyValueStoreDimension, "tarantula", "Spiders have eight legs"),
                makeDimensionRow(keyValueStoreDimension, "wolfspider", "Spiders have eight legs"),
                makeDimensionRow(keyValueStoreDimension, "crocodile", "A secret agent's worst fear"),
                makeDimensionRow(keyValueStoreDimension, "alligator", "A secret agent's worst fear"),
                makeDimensionRow(keyValueStoreDimension, "完成关卡", "Stage completed"),
                makeDimensionRow(keyValueStoreDimension, "aneurysm", "A secret agent's worst fear")
        ]
        dimensionRows.each {
            keyValueStoreDimension.addDimensionRow(it)
        }

        animalTable = Mock(LogicalTable)
        animalTable.getDimensions() >> [keyValueStoreDimension]
        spaceIdDictionary = Mock(DimensionDictionary)
        spaceIdDictionary.findByApiName(_) >> keyValueStoreDimension

        childSetup()
    }

    def cleanup() {
        MapStoreManager.removeInstance("animal")
        cleanSearchProvider("animal")
        childCleanup()
    }

    /**
     * Override this method with any child-specific setup the child class needs to perform.
     */
    void childSetup() {}

    /**
     * Override this method with any child-specific cleanup the child class needs to perform.
     */
    void childCleanup() {}


    /**
     * Override this method with any child-specific Spec level setup that the child class needs to perform
     */
    void childSetupSpec() {}

    /**
     * Override this method with any child-specific Spec-level cleanup
     */
    void childCleanupSpec() {}

    /**
     * Override this method to return the SearchProvider under test.
     * @param dimensionName  The name of the dimension to search
     */
    abstract T getSearchProvider(String dimensionName)

    /**
     * Override this with the code to clear the search provider.
     *
     * @param dimensionName  The name of the dimension that was searched
     */
    abstract void cleanSearchProvider(String dimensionName)

    def "getDimensionCardinality returns cardinality count"() {
        expect:
        searchProvider.getDimensionCardinality() == dimensionRows.size()
    }

    def "findAllDimensionRows reflects all rows when adding and updating dimension rows"() {
        given: "Some dimension fields"
        LinkedHashSet<DimensionField> dimensionUserCountryFields = [ID, DESC, FIELD1, FIELD2]

        and: "A dimension"
        Dimension dimensionUserCountry = new KeyValueStoreDimension(
                "user_country",
                "user_country-description",
                dimensionUserCountryFields,
                MapStoreManager.getInstance("user_country"),
                getSearchProvider("user_country")
        )
        dimensionUserCountry.setLastUpdated(new DateTime(50000))

        and: "Some rows"
        DimensionRow dimensionRowUSA = makeDimensionRow(
                dimensionUserCountry,
                "usa",
                "USA",
                "usa1",
                "usa2"
        )
        DimensionRow dimensionRowIndia = makeDimensionRow(
                dimensionUserCountry,
                "ind",
                "India",
                "ind1",
                "ind2"
        )

        dimensionUserCountry.addAllDimensionRows([dimensionRowIndia, dimensionRowUSA] as Set)

        and: "A modified row that already exists, a non-modified row that already exists, and a new row"
        DimensionRow modificationOfExistingRow = makeDimensionRow(
                dimensionUserCountry,
                "usa",
                "United_States_of_America",
                "foo",
                "usa2"
        )
        //Row that doesn't already exist
        DimensionRow canada = makeDimensionRow(
                dimensionUserCountry,
                "canada",
                "C_A_N_A_D_A",
                "bar",
                "cancancan"
        )

        Set<DimensionRow> expectedDimensionRowsUserCountryWithoutCanada = [
                modificationOfExistingRow,
                dimensionRowIndia,
        ]
        Set<DimensionRow> expectedDimensionRowsUserCountry = [modificationOfExistingRow, dimensionRowIndia, canada]

        when: "We update a row and add an already existing one"
        dimensionUserCountry.addAllDimensionRows(expectedDimensionRowsUserCountryWithoutCanada)
        PaginationParameters paginationParameters = new PaginationParameters(2, 1)

        then: "We get the 2 rows, one as it was and the other as the updated row"
        dimensionUserCountry.searchProvider.findAllDimensionRows() == expectedDimensionRowsUserCountryWithoutCanada
        dimensionUserCountry.searchProvider.findAllDimensionRows().size() == 2
        dimensionUserCountry.searchProvider.findAllDimensionRowsPaged(paginationParameters).getPageOfData() == new ArrayList(new TreeSet<>(expectedDimensionRowsUserCountryWithoutCanada))
        dimensionUserCountry.searchProvider.findAllDimensionRowsPaged(paginationParameters).getPageOfData().size() == 2

        when: "We next add a new row"
        dimensionUserCountry.addAllDimensionRows([canada] as Set)
        paginationParameters = new PaginationParameters(3, 1)

        then: "We get all 3 rows"
        dimensionUserCountry.searchProvider.findAllDimensionRows() == expectedDimensionRowsUserCountry
        dimensionUserCountry.searchProvider.findAllDimensionRows().size() == 3
        dimensionUserCountry.searchProvider.findAllDimensionRowsPaged(paginationParameters).getPageOfData() == new ArrayList(new TreeSet<>(expectedDimensionRowsUserCountry))
        dimensionUserCountry.searchProvider.findAllDimensionRowsPaged(paginationParameters).getPageOfData().size() == 3

        cleanup:
        dimensionUserCountry.searchProvider.clearDimension()
    }

    def "A filter query with 'startswith' and 'notin' on the same field returns a non-empty subset"() {
        setup:
        Set<ApiFilter> filters = [
                buildFilter("animal|desc-startswith[this]"),
                buildFilter("animal|desc-notin[this is an owl]")
        ]

        TreeSet<DimensionRow> expectedRows = [
                makeDimensionRow(keyValueStoreDimension, "hawk", "this is a raptor"),
                makeDimensionRow(keyValueStoreDimension, "eagle", "this is a raptor"),
                makeDimensionRow(keyValueStoreDimension, "kumquat", "this is not an animal"),
        ] as TreeSet

        expect:
        searchProvider.findFilteredDimensionRows(filters) == expectedRows
        searchProvider.findFilteredDimensionRowsPaged(filters, new PaginationParameters(3, 1)).getPageOfData() == new ArrayList<>(expectedRows)
    }

    def "A filter query with 'startswith' and 'in' on the same field returns a non-empty subset"() {
        setup:
        Set<ApiFilter> filters = [
                buildFilter("animal|desc-startswith[this]"),
                buildFilter("animal|desc-in[this is not an animal]")
        ]

        and:
        TreeSet<DimensionRow> expectedRows = [
                makeDimensionRow(keyValueStoreDimension, "kumquat", "this is not an animal")
        ] as TreeSet

        expect:
        searchProvider.findFilteredDimensionRows(filters) == expectedRows
        searchProvider.findFilteredDimensionRowsPaged(filters, new PaginationParameters(1, 1)).getPageOfData() == new ArrayList<>(expectedRows)
    }

    def "A filter query with 'in' filters on different fields returns a non-empty subset"() {
        given:
        Set<ApiFilter> filters = [
                buildFilter("animal|id-in[chimpanzee,spidermonkey,tarantula]"),
                buildFilter("animal|desc-in[Monkeys have teeth]")
        ]

        and:
        TreeSet<DimensionRow> expectedRows = [
                makeDimensionRow(keyValueStoreDimension, "chimpanzee", "Monkeys have teeth"),
                makeDimensionRow(keyValueStoreDimension, "spidermonkey", "Monkeys have teeth")
        ] as TreeSet

        expect:
        searchProvider.findFilteredDimensionRows(filters) == expectedRows
        searchProvider.findFilteredDimensionRowsPaged(filters, new PaginationParameters(2, 1)).getPageOfData() == new ArrayList<>(expectedRows)
    }

    def "A filter query with 'in' filters on different fields returns an empty subset"() {
        given:
        Set<ApiFilter> filters = [
                buildFilter("animal|id-in[chimpanzee,bonobo,spidermonkey]"),
                buildFilter("animal|desc-in[Spiders have eight legs]")
        ]

        expect:
        searchProvider.findFilteredDimensionRows(filters) == [] as TreeSet
        searchProvider.findFilteredDimensionRowsPaged(filters, new PaginationParameters(1, 1)).getPageOfData() == [] as List
    }

    def "A filter query with 'in' and 'notin' filters on different fields returns a non-empty subset"() {
        given:
        Set<ApiFilter> filters = [
                buildFilter("animal|id-in[chimpanzee,brownrecluse,alligator]"),
                buildFilter("animal|desc-notin[Spiders have eight legs,A secret agent's worst fear]")
        ]

        and:
        TreeSet<DimensionRow> expectedResults = [
                makeDimensionRow(keyValueStoreDimension, "chimpanzee", "Monkeys have teeth")
        ] as TreeSet

        expect:
        searchProvider.findFilteredDimensionRows(filters) == expectedResults
        searchProvider.findFilteredDimensionRowsPaged(filters, new PaginationParameters(1, 1)).getPageOfData() == new ArrayList<>(expectedResults)
    }

    def "A filter query with 'in' and 'notin' filters on different fields returns an empty subset"() {
        given:
        Set<ApiFilter> filters = [
                buildFilter("animal|id-in[brownrecluse,alligator]"),
                buildFilter("animal|desc-notin[Spiders have eight legs,A secret agent's worst fear]")
        ]

        expect:
        searchProvider.findFilteredDimensionRows(filters) == [] as TreeSet
        searchProvider.findFilteredDimensionRowsPaged(filters, new PaginationParameters(1, 1)).getPageOfData() == [] as List
    }

    def "A filter query with 'in' and 'notin' on one field, and 'in' on a second returns a non-empty subset" () {
        given:
        Set<ApiFilter> filters = [
                buildFilter("animal|id-in[chimpanzee,brownrecluse,wolfspider]"),
                buildFilter("animal|id-notin[brownrecluse]"),
                buildFilter("animal|desc-in[Spiders have eight legs]")
        ]

        and:
        TreeSet<DimensionRow> expectedResults = [
                makeDimensionRow(keyValueStoreDimension, "wolfspider", "Spiders have eight legs")
        ] as TreeSet

        expect:
        searchProvider.findFilteredDimensionRows(filters) == expectedResults
        searchProvider.findFilteredDimensionRowsPaged(filters, new PaginationParameters(1, 1)).getPageOfData() == new ArrayList<>(expectedResults)
    }

    def "A filter query with 'in' and 'notin' on one field, and 'notin' on a second returns a non-empty subset" () {
        given:
        Set<ApiFilter> filters = [
                buildFilter("animal|id-in[chimpanzee,bonobo,brownrecluse,tarantula]"),
                buildFilter("animal|id-notin[bonobo]"),
                buildFilter("animal|desc-notin[Spiders have eight legs]")
        ]

        and:
        TreeSet<DimensionRow> expectedResults = [
                makeDimensionRow(keyValueStoreDimension, "chimpanzee", "Monkeys have teeth")
        ] as TreeSet

        expect:
        searchProvider.findFilteredDimensionRows(filters) == expectedResults
        searchProvider.findFilteredDimensionRowsPaged(filters, new PaginationParameters(1, 1)).getPageOfData() == new ArrayList<>(expectedResults)
    }

    def "A filter query with 'in' and 'notin' on the same field returns a non-empty subset" () {
        given:
        Set<ApiFilter> filters = [
                buildFilter("animal|id-in[chimpanzee,bonobo,brownrecluse,tarantula]"),
                buildFilter("animal|id-notin[bonobo,brownrecluse]")
        ]

        and:
        TreeSet<DimensionRow> expectedResults = [
                makeDimensionRow(keyValueStoreDimension, "chimpanzee", "Monkeys have teeth"),
                makeDimensionRow(keyValueStoreDimension, "tarantula", "Spiders have eight legs")
        ] as TreeSet

        expect:
        searchProvider.findFilteredDimensionRows(filters) == expectedResults
        searchProvider.findFilteredDimensionRowsPaged(filters, new PaginationParameters(2, 1)).getPageOfData() == new ArrayList<>(expectedResults)
    }

    def "A filter query with two 'in' and 'in' on the same field returns a non-empty subset" () {
        given:
        Set<ApiFilter> filters = [
                buildFilter("animal|id-in[chimpanzee,bonobo,tarantula]"),
                buildFilter("animal|id-in[bonobo,brownrecluse,tarantula]")
        ]

        and:
        TreeSet<DimensionRow> expectedResults = [
                makeDimensionRow(keyValueStoreDimension, "bonobo", "Monkeys have teeth"),
                makeDimensionRow(keyValueStoreDimension, "tarantula", "Spiders have eight legs")
        ] as TreeSet

        expect:
        searchProvider.findFilteredDimensionRows(filters) == expectedResults
        searchProvider.findFilteredDimensionRowsPaged(filters, new PaginationParameters(2, 1)).getPageOfData() == new ArrayList<>(expectedResults)
    }

    def "A filter query with 'in' and 'in' on the same field returns an empty subset" () {
        given:
        Set<ApiFilter> filters = [
                buildFilter("animal|id-in[chimpanzee,bonobo]"),
                buildFilter("animal|id-in[brownrecluse,tarantula]")
        ]

        expect:
        searchProvider.findFilteredDimensionRows(filters) == [] as TreeSet
        searchProvider.findFilteredDimensionRowsPaged(filters, new PaginationParameters(1, 1)).getPageOfData() == [] as List
    }

    def "A filter query with 'notin' and 'notin' on the same field returns a non-empty subset" () {
        given:
        Set<ApiFilter> filters = [
                buildFilter("animal|id-notin[chimpanzee,bonobo]"),
                buildFilter("animal|id-notin[brownrecluse,tarantula]")
        ]

        TreeSet<DimensionRow> expectedResults = [
                makeDimensionRow(keyValueStoreDimension, "spidermonkey", "Monkeys have teeth"),
                makeDimensionRow(keyValueStoreDimension, "wolfspider", "Spiders have eight legs"),
                makeDimensionRow(keyValueStoreDimension, "crocodile", "A secret agent's worst fear"),
                makeDimensionRow(keyValueStoreDimension, "alligator", "A secret agent's worst fear"),
                makeDimensionRow(keyValueStoreDimension, "aneurysm", "A secret agent's worst fear"),
                makeDimensionRow(keyValueStoreDimension, "owl", "this is an owl"),
                makeDimensionRow(keyValueStoreDimension, "hawk", "this is a raptor"),
                makeDimensionRow(keyValueStoreDimension, "eagle", "this is a raptor"),
                makeDimensionRow(keyValueStoreDimension, "完成关卡", "Stage completed"),
                makeDimensionRow(keyValueStoreDimension, "kumquat", "this is not an animal")
        ] as TreeSet

        expect:
        searchProvider.findFilteredDimensionRows(filters) == expectedResults
        new TreeSet<Dimension>(searchProvider.findFilteredDimensionRowsPaged(filters, new PaginationParameters(10, 1)).getPageOfData()) == expectedResults
    }

    def "A filter query with 'notin' and 'notin' on the same field returns an empty subset" () {
        given:
        Set<ApiFilter> filters = [
                buildFilter("animal|id-notin[chimpanzee,bonobo,spidermonkey,完成关卡]"),
                buildFilter("animal|id-notin[brownrecluse,tarantula,wolfspider]"),
                buildFilter("animal|id-notin[crocodile,alligator,aneurysm]"),
                buildFilter("animal|id-notin[owl,hawk,eagle,kumquat]")
        ]
        expect:
        searchProvider.findFilteredDimensionRows(filters) == [] as TreeSet
        searchProvider.findFilteredDimensionRowsPaged(filters, new PaginationParameters(1, 1)).getPageOfData() == [] as List
    }

    def "A filter query with both 'notin' and 'in' on two fields returns a non-empty subset" () {
        given:
        Set<ApiFilter> filters = [
                buildFilter("animal|id-in[chimpanzee,bonobo,brownrecluse,tarantula,crocodile,alligator]"),
                buildFilter("animal|id-notin[chimpanzee,crocodile]"),
                buildFilter("animal|desc-in[Monkeys have teeth,A secret agent's worst fear]"),
                buildFilter("animal|desc-notin[Monkeys have teeth]")
        ]

        and:
        TreeSet<DimensionRow> expectedResponse = [
                makeDimensionRow(keyValueStoreDimension, "alligator", "A secret agent's worst fear")
        ] as TreeSet

        expect:
        searchProvider.findFilteredDimensionRows(filters) == expectedResponse
        new TreeSet<>(searchProvider.findFilteredDimensionRowsPaged(filters, new PaginationParameters(1, 1)).getPageOfData()) == expectedResponse
    }

    def "resetIndices clears the indices"() {
        when:
        searchProvider.clearDimension()

        then:
        indicesHaveBeenCleared()
    }

    def "updating a row refreshes index"() {
        given: "The filters used to access the refreshed dimension rows"
        ApiFilter oldDescription = new ApiFilter(
                keyValueStoreDimension,
                DESC,
                FilterOperation.eq,
                ["this is a raptor"] as Set
        )
        ApiFilter newDescription = new ApiFilter(
                keyValueStoreDimension,
                DESC,
                FilterOperation.eq,
                ["this is a new raptor"] as Set
        )

        expect: "There are two dimension rows with the old description"
        searchProvider.findFilteredDimensionRows([oldDescription] as Set) == [dimensionRow2, dimensionRow2a] as Set
        searchProvider.findFilteredDimensionRowsPaged([oldDescription] as Set, new PaginationParameters(2, 1)).getPageOfData() == [dimensionRow2a, dimensionRow2] as List

        when: "We add a new dimension row with the new description and the same id as dimensionRow2"
        DimensionRow dimensionRow2new = makeDimensionRow(
                keyValueStoreDimension,
                "hawk",
                "this is a new raptor"
        )
        keyValueStoreDimension.addDimensionRow(dimensionRow2new)
        PaginationParameters paginationParameters = new PaginationParameters(1, 1)

        then: "There is now one row with the old description, and one row with the new description"
        searchProvider.findFilteredDimensionRows([oldDescription] as Set) == [dimensionRow2a] as Set
        searchProvider.findFilteredDimensionRows([newDescription] as Set) == [dimensionRow2new] as Set
        searchProvider.findFilteredDimensionRowsPaged([oldDescription] as Set, paginationParameters).getPageOfData() == [dimensionRow2a] as List
        searchProvider.findFilteredDimensionRowsPaged([newDescription] as Set, paginationParameters).getPageOfData() == [dimensionRow2new] as List
    }

    /**
     * Checks that this search provider's indices have been cleared.
     *
     * @return true if the search provider's indices have been cleared properly, false otherwise
     */
    abstract boolean indicesHaveBeenCleared();

    ApiFilter buildFilter(String filterQuery) {
        new ApiFilter(filterQuery, animalTable, spaceIdDictionary)
    }

    def "findAllDimensionRowsPaged and findFilteredDimensionRowsPaged paginates results correctly"() {
        given: "Some dimension fields"
        LinkedHashSet<DimensionField> dimensionUserCountryFields = [ID, DESC, FIELD1, FIELD2]

        and: "A dimension"
        Dimension dimensionUserCountry = new KeyValueStoreDimension(
                "user_country",
                "user_country-description",
                dimensionUserCountryFields,
                MapStoreManager.getInstance("user_country"),
                getSearchProvider("user_country")
        )
        dimensionUserCountry.setLastUpdated(new DateTime(50000))

        and: "Some rows"
        DimensionRow dimensionRow1 = makeDimensionRow(
                dimensionUserCountry,
                "country1",
                "Country1",
                "c11",
                "c12"
        )
        DimensionRow dimensionRow2 = makeDimensionRow(
                dimensionUserCountry,
                "country2",
                "Country2",
                "c21",
                "c22"
        )
        DimensionRow dimensionRow3 = makeDimensionRow(
                dimensionUserCountry,
                "country3",
                "Country3",
                "c31",
                "c32"
        )
        DimensionRow dimensionRow4 = makeDimensionRow(
                dimensionUserCountry,
                "country4",
                "Country4",
                "c41",
                "c42"
        )
        DimensionRow dimensionRow5 = makeDimensionRow(
                dimensionUserCountry,
                "country5",
                "Country5",
                "c51",
                "c52"
        )
        DimensionRow dimensionRow6 = makeDimensionRow(
                dimensionUserCountry,
                "country6",
                "Country6",
                "c61",
                "c62"
        )

        when: "We request the first page that is empty"
        dimensionUserCountry.addAllDimensionRows([] as Set)
        Set<DimensionRow> expectedResults = [];
        PaginationParameters paginationParameters = new PaginationParameters(2, 1)
        Set<ApiFilter> filters = [
                buildFilter("user_country|id-in[country1]")
        ]

        then: "We get an empty first page"
        dimensionUserCountry.searchProvider.findAllDimensionRowsPaged(paginationParameters).getPageOfData() == new ArrayList(new TreeSet<>(expectedResults))
        dimensionUserCountry.searchProvider.findAllDimensionRowsPaged(paginationParameters).getPageOfData().size() == 0
        dimensionUserCountry.searchProvider.findFilteredDimensionRowsPaged(filters, paginationParameters).getPageOfData() == new ArrayList(new TreeSet<>(expectedResults))
        dimensionUserCountry.searchProvider.findFilteredDimensionRowsPaged(filters, paginationParameters).getPageOfData().size() == 0

        when: "we request the first page that is not full"
        dimensionUserCountry.addAllDimensionRows([dimensionRow1, dimensionRow2] as Set)
        expectedResults = [dimensionRow1];
        paginationParameters = new PaginationParameters(1, 1)
        filters = [
                buildFilter("user_country|id-in[country1]")
        ]

        then: "We get an first page that is not full"
        dimensionUserCountry.searchProvider.findAllDimensionRowsPaged(paginationParameters).getPageOfData() == new ArrayList(new TreeSet<>(expectedResults))
        dimensionUserCountry.searchProvider.findAllDimensionRowsPaged(paginationParameters).getPageOfData().size() == 1
        dimensionUserCountry.searchProvider.findFilteredDimensionRowsPaged(filters, paginationParameters).getPageOfData() == new ArrayList(new TreeSet<>(expectedResults))
        dimensionUserCountry.searchProvider.findFilteredDimensionRowsPaged(filters, paginationParameters).getPageOfData().size() == 1

        when: "we request the first page that is full"
        dimensionUserCountry.addAllDimensionRows([dimensionRow3, dimensionRow4, dimensionRow5, dimensionRow6] as Set)
        expectedResults = [dimensionRow1, dimensionRow2];
        paginationParameters = new PaginationParameters(2, 1)
        filters = [
                buildFilter("user_country|id-in[country1,country2]")
        ]

        then: "We get an full first page"
        dimensionUserCountry.searchProvider.findAllDimensionRowsPaged(paginationParameters).getPageOfData() == new ArrayList(new TreeSet<>(expectedResults))
        dimensionUserCountry.searchProvider.findAllDimensionRowsPaged(paginationParameters).getPageOfData().size() == 2
        dimensionUserCountry.searchProvider.findFilteredDimensionRowsPaged(filters, paginationParameters).getPageOfData() == new ArrayList(new TreeSet<>(expectedResults))
        dimensionUserCountry.searchProvider.findFilteredDimensionRowsPaged(filters, paginationParameters).getPageOfData().size() == 2

        when: "we request a middle page that is full"
        expectedResults = [dimensionRow3, dimensionRow4];
        paginationParameters = new PaginationParameters(2, 2)
        filters = [
                buildFilter("user_country|id-in[country1,country2,country3,country4]")
        ]

        then: "We get an full middle page"
        dimensionUserCountry.searchProvider.findAllDimensionRowsPaged(paginationParameters).getPageOfData() == new ArrayList(new TreeSet<>(expectedResults))
        dimensionUserCountry.searchProvider.findAllDimensionRowsPaged(paginationParameters).getPageOfData().size() == 2
        dimensionUserCountry.searchProvider.findFilteredDimensionRowsPaged(filters, paginationParameters).getPageOfData() == new ArrayList(new TreeSet<>(expectedResults))
        dimensionUserCountry.searchProvider.findFilteredDimensionRowsPaged(filters, paginationParameters).getPageOfData().size() == 2

        when: "we request the last page that is not full"
        dimensionUserCountry.searchProvider.clearDimension();
        dimensionUserCountry.addAllDimensionRows([dimensionRow1, dimensionRow2, dimensionRow3, dimensionRow4, dimensionRow5] as Set)
        expectedResults = [dimensionRow5];
        paginationParameters = new PaginationParameters(2, 3)
        filters = [
                buildFilter("user_country|id-in[country1,country2,country3,country4,country5]")
        ]

        then: "We get a partial last page"
        dimensionUserCountry.searchProvider.findAllDimensionRowsPaged(paginationParameters).getPageOfData() == new ArrayList(new TreeSet<>(expectedResults))
        dimensionUserCountry.searchProvider.findAllDimensionRowsPaged(paginationParameters).getPageOfData().size() == 1
        dimensionUserCountry.searchProvider.findFilteredDimensionRowsPaged(filters, paginationParameters).getPageOfData() == new ArrayList(new TreeSet<>(expectedResults))
        dimensionUserCountry.searchProvider.findFilteredDimensionRowsPaged(filters, paginationParameters).getPageOfData().size() == 1

        when: "we request the last page that is full"
        dimensionUserCountry.addAllDimensionRows([dimensionRow6] as Set)
        expectedResults = [dimensionRow5, dimensionRow6];
        paginationParameters = new PaginationParameters(2, 3)
        filters = [
                buildFilter("user_country|id-in[country1,country2,country3,country4,country5,country6]")
        ]

        then: "We get a full last page"
        dimensionUserCountry.searchProvider.findAllDimensionRowsPaged(paginationParameters).getPageOfData() == new ArrayList(new TreeSet<>(expectedResults))
        dimensionUserCountry.searchProvider.findAllDimensionRowsPaged(paginationParameters).getPageOfData().size() == 2
        dimensionUserCountry.searchProvider.findFilteredDimensionRowsPaged(filters, paginationParameters).getPageOfData() == new ArrayList(new TreeSet<>(expectedResults))
        dimensionUserCountry.searchProvider.findFilteredDimensionRowsPaged(filters, paginationParameters).getPageOfData().size() == 2

        when: "we request the first page"
        expectedResults = [dimensionRow1, dimensionRow2];
        paginationParameters = new PaginationParameters(2, 1)
        filters = [
                buildFilter("user_country|id-in[country1,country2,country3,country4]")
        ]

        then: "We get a full first page"
        dimensionUserCountry.searchProvider.findFilteredDimensionRowsPaged(filters, paginationParameters).getPageOfData() == new ArrayList(new TreeSet<>(expectedResults))
        dimensionUserCountry.searchProvider.findFilteredDimensionRowsPaged(filters, paginationParameters).getPageOfData().size() == 2

        when: "we request a middle page"
        expectedResults = [dimensionRow3];
        paginationParameters = new PaginationParameters(1, 2)
        filters = [
                buildFilter("user_country|id-in[country2,country3,country4]")
        ]

        then: "We get a correct middle page"
        dimensionUserCountry.searchProvider.findFilteredDimensionRowsPaged(filters, paginationParameters).getPageOfData() == new ArrayList(new TreeSet<>(expectedResults))
        dimensionUserCountry.searchProvider.findFilteredDimensionRowsPaged(filters, paginationParameters).getPageOfData().size() == 1

        when: "we request the last page"
        expectedResults = [dimensionRow4];
        paginationParameters = new PaginationParameters(1, 3)
        filters = [
                buildFilter("user_country|id-in[country2,country3,country4]")
        ]

        then: "We get the correct last page"
        dimensionUserCountry.searchProvider.findFilteredDimensionRowsPaged(filters, paginationParameters).getPageOfData() == new ArrayList(new TreeSet<>(expectedResults))
        dimensionUserCountry.searchProvider.findFilteredDimensionRowsPaged(filters, paginationParameters).getPageOfData().size() == 1

        cleanup:
        dimensionUserCountry.searchProvider.clearDimension()
    }
}
