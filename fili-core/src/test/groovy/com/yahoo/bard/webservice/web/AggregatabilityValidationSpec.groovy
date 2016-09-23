// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import com.yahoo.bard.webservice.table.PhysicalTable
import org.joda.time.DateTimeZone

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.TableGroup

import org.joda.time.DateTime

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.core.MultivaluedHashMap
import javax.ws.rs.core.PathSegment

class AggregatabilityValidationSpec extends Specification {

    @Shared
    DimensionDictionary dimensionDict
    @Shared
    LogicalTable table
    @Shared
    Map emptyMap = new MultivaluedHashMap<>()

    def setup() {
        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>()
        dimensionFields.add(BardDimensionField.ID)
        dimensionFields.add(BardDimensionField.DESC)

        dimensionDict = new DimensionDictionary()
        KeyValueStoreDimension keyValueStoreDimension
        ["locale", "one", "two"].each { String name ->
            keyValueStoreDimension = new KeyValueStoreDimension(
                    name,
                    "desc-" + name,
                    dimensionFields,
                    MapStoreManager.getInstance(name),
                    ScanSearchProviderManager.getInstance(name)
            )
            keyValueStoreDimension.setLastUpdated(new DateTime(10000))
            dimensionDict.add(keyValueStoreDimension)
        }
        ["three", "four", "five"].each { String name ->
            keyValueStoreDimension = new KeyValueStoreDimension(
                    name,
                    "druid-" + name,
                    Dimension.DEFAULT_CATEGORY,
                    "desc" + name,
                    dimensionFields,
                    MapStoreManager.getInstance(name),
                    ScanSearchProviderManager.getInstance(name),
                    new LinkedHashSet<DimensionField>(),
                    false
            )
            keyValueStoreDimension.setLastUpdated(new DateTime(10000))
            dimensionDict.add(keyValueStoreDimension)
        }
        TableGroup tg = Mock(TableGroup)
        tg.getDimensions() >> dimensionDict.apiNameToDimension.values()
        table = new LogicalTable("name", DAY, tg)
        dimensionDict.apiNameToDimension.values().each {
            DimensionColumn.addNewDimensionColumn(table, it, new PhysicalTable("abc", DAY.buildZonedTimeGrain(DateTimeZone.UTC), [:]))
        }
    }

    @Unroll
    def "Aggregatability validates successfully with #aggSize aggregatable and #nonAggSize non-aggregatable group by dimensions and #hasFilter filter#filterFormat"() {
        setup:
        DataApiRequest apiRequest = new DataApiRequest();
        Set<Dimension> dims = apiRequest.generateDimensions(aggDims + nonAggDims, dimensionDict)
        Map<Dimension, Set<ApiFilter>> filters = apiRequest.generateFilters(filterString, table, dimensionDict)

        when:
        apiRequest.validateAggregatability(dims, filters)

        then:
        notThrown BadApiRequestException

        where:
        aggDimStrings  | nonAggDimStrings  | filterString                                                            | testCase
        []             | []                | ""                                                                      | "No dimensions, no filters"
        ["one"]        | []                | ""                                                                      | "1 agg, no filters"
        []             | ["four"]          | ""                                                                      | "1 non-agg, no filters"
        ["one"]        | ["four"]          | ""                                                                      | "1 agg, 1 non-agg, no filters"
        ["one"]        | ["three", "four"] | ""                                                                      | "1 agg, 2 non-aggs, no filters"
        ["one", "two"] | ["three"]         | ""                                                                      | "2 aggs, 1 non-aggs, no filters"
        ["one", "two"] | ["three", "four"] | ""                                                                      | "2 aggs, 2 non-aggs, no filters"

        []             | []                | "one|id-in[blue]"                                                       | "1 value filter in an agg"
        []             | []                | "one|id-in[blue,red]"                                                   | "2 values filter in an agg"
        []             | []                | "four|id-in[cat]"                                                       | "2 values filter in an agg"
        []             | []                | "one|id-in[blue,red],four|id-in[cat]"                                   | "2 values filter in an agg, 1 value filter in a non-agg"

        ["one"]        | ["four"]          | "one|id-in[blue,red]"                                                   | "1 agg, 1 non-agg, 2 values filter in the agg"
        ["one"]        | ["four"]          | "four|id-in[cat]"                                                       | "1 agg, 1 non-agg, 1 value filter in the non-agg"
        ["one"]        | ["four"]          | "four|id-in[cat,dog]"                                                   | "1 agg, 1 non-agg, 2 values filter in the non-agg"
        ["one"]        | ["four"]          | "three|id-in[3]"                                                        | "1 agg, 1 non-agg, 1 value filter in another non-agg"
        ["one"]        | ["four"]          | "two|id-in[F]"                                                          | "1 agg, 1 non-agg, 1 value filter in another agg"
        ["one"]        | ["four"]          | "two|id-in[M,F]"                                                        | "1 agg, 1 non-agg, 2 values filter in another agg"

        ["one"]        | ["four"]          | "one|id-in[blue]"                                                       | "1 agg, 1 non-agg, 1 value filter in the agg"
        ["one"]        | ["four"]          | "one|id-in[blue,red]"                                                   | "1 agg, 1 non-agg, 2 values filter in the agg"
        ["one"]        | ["four"]          | "four|id-in[cat]"                                                       | "1 agg, 1 non-agg, 1 value filter in the non-agg"
        ["one"]        | ["four"]          | "four|id-in[cat,dog]"                                                   | "1 agg, 1 non-agg, 2 values filter in the non-agg"
        ["one"]        | ["four"]          | "three|id-in[3]"                                                        | "1 agg, 1 non-agg, 1 value filter in another non-agg"
        ["one"]        | ["four"]          | "two|id-in[F]"                                                          | "1 agg, 1 non-agg, 1 value filter in another agg"
        ["one"]        | ["four"]          | "two|id-in[M,F]"                                                        | "1 agg, 1 non-agg, 2 values filter in another agg"

        ["one"]        | ["four"]          | "one|id-in[blue],four|id-in[cat]"                                       | "1 agg, 1 non-agg, 1 value filter in the agg, 1 value filter in the non-agg"
        ["one"]        | ["four"]          | "one|id-in[blue,red],four|id-in[cat]"                                   | "1 agg, 1 non-agg, 2 values filter in the agg, 1 value filter in the non-agg"
        ["one"]        | ["four"]          | "one|id-in[blue],four|id-in[cat,dog]"                                   | "1 agg, 1 non-agg, 1 value filter in the agg, 2 values filter in the non-agg"
        ["one"]        | ["four"]          | "one|id-in[blue,red],four|id-in[cat,dog]"                               | "1 agg, 1 non-agg, 2 values filter in the agg, 2 values filter in the non-agg"

        ["one"]        | ["four"]          | "one|id-in[blue,red],two|id-in[M,F],four|id-in[cat]"                    | "1 agg, 1 non-agg, 2 values filter in the agg, 2 values filter in the non-agg, 2 values filter in another agg"
        ["one"]        | ["four"]          | "one|id-in[blue,red],two|id-in[M,F],four|id-in[cat],three|id-in[3]"     | "1 agg, 1 non-agg, 2 values filter in the agg, 1 value filter in the non-agg, 2 values filter in another agg, 1 value filter in another non-agg"
        ["one"]        | ["four"]          | "one|id-in[blue,red],two|id-in[M,F],four|id-in[cat,dog],three|id-in[3]" | "1 agg, 1 non-agg, 2 values filter in the agg, 2 values filter in the non-agg, 2 values filter in another agg, 1 value filter in another non-agg"
        ["one"]        | ["four"]          | "two|id-in[M,F],four|id-in[cat,dog],three|id-in[3]"                     | "1 agg, 1 non-agg, 2 values filter in the non-agg, 2 values filter in another agg, 1 value filter in another non-agg"

        ["one"]        | ["four"]          | "four|id-in[cat],three|id-in[3]"                                        | "1 agg, 1 non-agg, 1 value filter in the non-agg, 1 values filter in another non-agg"
        ["one"]        | ["four"]          | "four|id-in[cat,dog],three|id-in[3]"                                    | "1 agg, 1 non-agg, 2 values filter in the non-agg, 1 value filter in another non-agg"
        ["one"]        | ["four"]          | "one|id-in[blue,red],two|id-in[M]"                                      | "1 agg, 1 non-agg, 1 value filter in the agg, 1 values filter in another agg"

        ["one", "two"] | ["four"]          | "two|id-in[M,F],three|id-in[3],four|id-in[cat,dog]"                     | "2 aggs, 1 non-agg, 2 values filter in one agg, 2 values filter in the non-agg, 1 value filter in another non-agg"
        ["one", "two"] | ["four", "three"] | "two|id-in[M,F],three|id-in[3],four|id-in[cat,dog]"                     | "2 aggs, 2 non-aggs, 2 values filter in one agg, 1 and 2 values filter in the non-aggs"
        ["one", "two"] | ["four", "three"] | "two|id-in[M,F],three|id-in[3],four|id-in[cat,dog],five|id-in[square]"  | "2 aggs, 2 non-aggs, 2 values filter in one agg, 1 and 2 values filter in the non-aggs, 1 value filter in another non-agg"

        ["one"]        | ["four"]          | "one|desc-in[blue],four|desc-in[cat]"                                   | "1 agg, 1 non-agg, 1 value filter in the agg, 1 value filter in the non-agg (not key)"
        ["one"]        | ["four"]          | "one|id-in[blue,red],four|desc-in[cat]"                                 | "1 agg, 1 non-agg, 2 values filter in the agg, 1 value filter in the non-agg (not key)"
        ["one"]        | ["four"]          | "one|id-in[blue],four|desc-in[cat,dog]"                                 | "1 agg, 1 non-agg, 1 value filter in the agg, 2 values filter in the non-agg (not key)"
        ["one"]        | ["four"]          | "one|id-in[blue,red],four|desc-in[cat,dog]"                             | "1 agg, 1 non-agg, 2 values filter in the agg, 2 values filter in the non-agg (not key)"

        aggDims = aggDimStrings.collect { String name ->
            Mock(PathSegment) {
                getPath() >> name
                getMatrixParameters() >> emptyMap
            }
        }

        nonAggDims = nonAggDimStrings.collect { String name ->
            Mock(PathSegment) {
                getPath() >> name
                getMatrixParameters() >> emptyMap
            }
        }

        aggSize = aggDims.size()
        nonAggSize = nonAggDims.size()
        hasFilter = filterString == "" ? "no" : ""
        filterFormat = filterString == "" ? "" : ": " + filterString
    }

    @Unroll
    def "Aggregatability validation throws #exception.simpleName with #aggSize aggregatable and #nonAggSize non-aggregatable group by dimensions and #hasFilter filter#filterFormat"() {
        setup:
        DataApiRequest apiRequest = new DataApiRequest();
        Set<Dimension> dims = apiRequest.generateDimensions(aggDims + nonAggDims, dimensionDict)
        Map<Dimension, Set<ApiFilter>> filters = apiRequest.generateFilters(filterString, table, dimensionDict)

        when:
        apiRequest.validateAggregatability(dims, filters)

        then:
        thrown exception

        where:
        aggDimStrings  | nonAggDimStrings  | filterString                                                            | testCase

        []             | []                | "four|id-in[cat,dog]"                                                   | "2 values filter in a non-agg"
        []             | []                | "four|desc-in[cat,dog]"                                                 | "2 values filter in a non-agg"
        []             | []                | "four|id-in[cat],three|id-in[3,7]"                                      | "1 and 2 values filter in non-aggs"
        []             | []                | "one|id-in[blue],four|id-in[cat,dog]"                                   | "1 value filter in a agg, 2 values filter in a non-agg"
        []             | []                | "one|id-in[blue],four|id-in[cat,dog],three|id-in[3,7]"                  | "1 value filter in a agg, 1 and 2 values filter in non-aggs"

        ["one"]        | ["four"]          | "three|id-in[3,7]"                                                      | "1 agg, 1 non-agg, 2 values filter in another non-agg"
        ["one"]        | ["four"]          | "three|desc-in[3]"                                                      | "1 agg, 1 non-agg, 1 value filter in another non-agg (but not key)"
        ["one"]        | ["four"]          | "four|desc-in[cat],three|desc-in[3,7]"                                  | "1 agg, 1 non-agg, 1 value filter in the non-agg, 2 values filter in another non-agg (not key)"
        ["one"]        | ["four"]          | "four|desc-in[cat,doc],three|desc-in[3,7]"                              | "1 agg, 1 non-agg, 2 values filter in the non-agg, 2 values filter in another non-agg (not key)"

        ["one"]        | ["four"]          | "one|id-in[blue,red],two|id-in[M,F],four|id-in[cat],three|id-in[3,7]"   | "1 agg, 1 non-agg, 2 values filter in the agg, 1 value filter in the non-agg, 2 values filter in another agg, 2 value filter in another non-agg"
        ["one"]        | ["four"]          | "one|id-in[blue,red],two|id-in[M],four|id-in[cat,dog],three|id-in[3,7]" | "1 agg, 1 non-agg, 2 values filter in the agg, 2 values filter in the non-agg, 1 value filter in another agg, 2 value filter in another non-agg"
        ["one"]        | ["four"]          | "two|id-in[M,F],four|id-in[cat,dog],three|id-in[3,7]"                   | "1 agg, 1 non-agg, 2 values filter in the non-agg, 2 values filter in another agg, 2 values filter in another non-agg"

        ["one"]        | ["four"]          | "four|id-in[cat],three|id-in[3,7]"                                      | "1 agg, 1 non-agg, 1 value filter in the non-agg, 2 values filter in another non-agg"
        ["one"]        | ["four"]          | "four|id-in[cat,dog],three|id-in[3,7]"                                  | "1 agg, 1 non-agg, 2 values filter in the non-agg, 2 values filter in another non-agg"

        ["one", "two"] | ["four", "five"] | "two|id-in[M,F],three|id-in[3,7],four|id-in[cat,dog],five|id-in[square]" | "2 aggs, 2 non-aggs, 2 values filter in one agg, 1 and 2 values filter in the non-aggs, 2 values filter in another non-agg"

        aggDims = aggDimStrings.collect { String name ->
            Mock(PathSegment) {
                getPath() >> name
                getMatrixParameters() >> emptyMap
            }
        }

        nonAggDims = nonAggDimStrings.collect { String name ->
            Mock(PathSegment) {
                getPath() >> name
                getMatrixParameters() >> emptyMap
            }
        }

        exception = BadApiRequestException
        aggSize = aggDims.size()
        nonAggSize = nonAggDims.size()
        hasFilter = filterString == "" ? "no" : ""
        filterFormat = filterString == "" ? "" : ": " + filterString
    }
}
