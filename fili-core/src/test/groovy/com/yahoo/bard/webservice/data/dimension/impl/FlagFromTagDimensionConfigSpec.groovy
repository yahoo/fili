// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl

import com.yahoo.bard.webservice.data.config.dimension.DefaultDimensionField
import com.yahoo.bard.webservice.data.config.dimension.FlagFromTagDimensionConfig
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionRow
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.CascadeExtractionFunction
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.LookupExtractionFunction
import com.yahoo.bard.webservice.util.DimensionStoreKeyUtils
import com.yahoo.bard.webservice.web.DefaultFilterOperation
import com.yahoo.bard.webservice.web.FilterOperation

import spock.lang.Specification
import spock.lang.Unroll

import java.util.stream.Collectors

class FlagFromTagDimensionConfigSpec extends Specification {

    DimensionDictionary dimensionDictionary

    String trueValue, falseValue

    def setup() {
        trueValue = "TRUE_VALUE"
        falseValue = "FALSE_VALUE"

        dimensionDictionary = new DimensionDictionary()
        // filtering dimension
        dimensionDictionary.add(Mock(Dimension) {getApiName() >> "filteringDimensionApiName"})

    }

    FlagFromTagDimensionConfig getConfig(List<ExtractionFunction> baseExtractionFunctions) {
        FlagFromTagDimensionConfig.Builder builder = new FlagFromTagDimensionConfig.Builder(
                {"flagFromTag"},
                "physicalName",
                "description",
                "longName",
                "category",
                "filteringDimensionApiName",
                "TAG_VALUE",
        )
        return builder
                .extractionFunctions(baseExtractionFunctions)
                .trueValue(trueValue)
                .falseValue(falseValue)
                .build()
    }

    def "Builder correctly sets user provided fields on dimension"() {
        setup:
        FlagFromTagDimensionConfig.Builder builder = new FlagFromTagDimensionConfig.Builder(
                {"flagFromTag"},
                "physicalName",
                "description",
                "longName",
                "category",
                "filteringDimensionApiName",
                "TAG_VALUE",
        )
        LinkedHashSet fields = [DefaultDimensionField.ID] as LinkedHashSet
        List<ExtractionFunction> baseExtractionFunctions = [Mock(ExtractionFunction), Mock(ExtractionFunction)]
        String testTrueValue = "testTrueValue"
        String testFalseValue = "testFalseValue"
        List<FilterOperation> positiveOps = [DefaultFilterOperation.in]
        List<FilterOperation> negativeOps = [DefaultFilterOperation.notin]
        FilterOperation positiveInvertOp = DefaultFilterOperation.in
        FilterOperation negativeInvertOp = DefaultFilterOperation.between

        when:
        FlagFromTagDimension dim = new FlagFromTagDimension(
                builder
                        .fields(fields)
                        .extractionFunctions(baseExtractionFunctions)
                        .trueValue(testTrueValue)
                        .falseValue(testFalseValue)
                        .positiveOps(positiveOps)
                        .negativeOps(negativeOps)
                        .positiveInvertedFilterOperation(positiveInvertOp)
                        .negativeInvertedFilterOperation(negativeInvertOp)
                        .build(),
                dimensionDictionary
        )

        then:
        dim.getDimensionFields() == fields
        [dim.getRegisteredLookupExtractionFns()[0], dim.getRegisteredLookupExtractionFns()[1]] == baseExtractionFunctions
        dim.getTrueValue() == testTrueValue
        dim.getFalseValue() == testFalseValue
        dim.positiveOps.stream().collect(Collectors.toSet()) == positiveOps.stream().collect(Collectors.toSet())
        dim.negativeOps.stream().collect(Collectors.toSet()) == negativeOps.stream().collect(Collectors.toSet())
        dim.positiveInvertedFilterOperation == positiveInvertOp
        dim.negativeInvertedFilterOperation == negativeInvertOp
    }

    def "builder errors if default show fields are not a subset of dimension fields"() {
        setup:
        FlagFromTagDimensionConfig.Builder builder = new FlagFromTagDimensionConfig.Builder(
                {"flagFromTag"},
                "physicalName",
                "description",
                "longName",
                "category",
                "filteringDimensionApiName",
                "TAG_VALUE",
        )
        LinkedHashSet<DimensionField> fields = [DefaultDimensionField.ID] as LinkedHashSet
        LinkedHashSet<DimensionField> showFields = [Mock(DimensionField)] as LinkedHashSet

        when:
        builder.fields(fields, showFields)

        then:
        thrown(IllegalArgumentException)
    }

    def "builder errors if provided with empty ops set"() {
        setup:
        FlagFromTagDimensionConfig.Builder builder = new FlagFromTagDimensionConfig.Builder(
                {"flagFromTag"},
                "physicalName",
                "description",
                "longName",
                "category",
                "filteringDimensionApiName",
                "TAG_VALUE",
        )

        when:
        builder.positiveOps([])

        then:
        thrown(IllegalArgumentException)

        when:
        builder.negativeOps([])

        then:
        thrown(IllegalArgumentException)
    }

    def "builder collects all individually configured lookup functions and flattens cascade extraction functions"() {
        setup:
        FlagFromTagDimensionConfig.Builder builder = new FlagFromTagDimensionConfig.Builder(
                {"flagFromTag"},
                "physicalName",
                "description",
                "longName",
                "category",
                "filteringDimensionApiName",
                "TAG_VALUE",
        )

        ExtractionFunction fn1 = Mock()
        ExtractionFunction fn2 = Mock()
        ExtractionFunction fn3 = Mock()
        ExtractionFunction fn4 = Mock()
        CascadeExtractionFunction cascadeFn = new CascadeExtractionFunction([fn2, fn3])

        when:
        FlagFromTagDimension dim = new FlagFromTagDimension(
                builder
                        .addExtractionFunction(fn1)
                        .addExtractionFunction(cascadeFn)
                        .addExtractionFunction(fn4)
                        .build(),
                dimensionDictionary
        )

        then:
        dim.getRegisteredLookupExtractionFns()[0] == fn1
        dim.getRegisteredLookupExtractionFns()[1] == fn2
        dim.getRegisteredLookupExtractionFns()[2] == fn3
        dim.getRegisteredLookupExtractionFns()[3] == fn4
    }

    @Unroll
    def "grouping dimension successfully constructed from RegisteredLookupDimension"() {
        when:
        Dimension fft = new FlagFromTagDimension(getConfig(extractionFunctions), dimensionDictionary)

        then:
        fft.getRegisteredLookupExtractionFns().size() == extractionFunctions.size() + 2
        fft.getSearchProvider() instanceof MapSearchProvider

        where:
        extractionFunctions << [
                [] as List<ExtractionFunction>,
                [Mock(CascadeExtractionFunction), Mock(LookupExtractionFunction)] as List<ExtractionFunction>,
        ]
    }

    def "Map store is created properly"() {
        when:
        Dimension fft = new FlagFromTagDimension(getConfig([]), dimensionDictionary)

        then:
        fft.getKeyValueStore().get(DimensionStoreKeyUtils.getRowKey(fft.getKey().getName(), trueValue)) == /{"id":"${trueValue}"}/
        fft.getKeyValueStore().get(DimensionStoreKeyUtils.getRowKey(fft.getKey().getName(), falseValue)) == /{"id":"${falseValue}"}/
    }

    def "Search provider is created properly"() {
        when:
        Dimension fft = new FlagFromTagDimension(getConfig([]), dimensionDictionary)
        Set<String> expectedValues = [trueValue, falseValue] as Set

        then:
        fft.getSearchProvider().findAllDimensionRows().stream().allMatch({ row -> expectedValues.remove(row.get(fft.getKey())) })
    }

    def "Attempting to mutate underlying dimension fails"() {
        setup:
        FlagFromTagDimension dim = new FlagFromTagDimension(
                new FlagFromTagDimensionConfig.Builder(
                        {"flagFromTag"},
                        "physicalName",
                        "description",
                        "longName",
                        "category",
                        "filteringDimensionApiName",
                        "TAG_VALUE",
                ).build(),
                dimensionDictionary
        )

        when:
        dim.addDimensionRow(Mock(DimensionRow))

        then:
        thrown(UnsupportedOperationException)

        when:
        dim.addAllDimensionRows([Mock(DimensionRow)] as Set)

        then:
        thrown(UnsupportedOperationException)
    }

    def "Equivalence works properly"(){
        setup:
        FlagFromTagDimensionConfig config = new FlagFromTagDimensionConfig.Builder(
                {"flagFromTag"},
                "physicalName",
                "description",
                "longName",
                "category",
                "filteringDimensionApiName",
                "TAG_VALUE",
        ).build()

        Dimension nonFft = Mock()
        FlagFromTagDimension fft1 = new FlagFromTagDimension(config, dimensionDictionary)
        FlagFromTagDimension fft2 = new FlagFromTagDimension(config, dimensionDictionary)

        expect:
        fft1 != nonFft

        and:
        fft1 == fft1
        fft1 == fft2

    }
}
