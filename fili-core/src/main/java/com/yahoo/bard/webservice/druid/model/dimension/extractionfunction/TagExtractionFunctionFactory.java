// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.dimension.extractionfunction;

import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tag Extraction Function handles a case where a comma delimited list of values is being filtered for the existence
 * of a particular tag value.
 *
 *  Given a target value 11, it should match:
 *  11
 *  11,12
 *  10,11
 *  10,11,12
 *
 *  and not match on values where 11 is only a substring:
 *  e.g.
 *  111
 */
public final class TagExtractionFunctionFactory {

    public static final String DEFAULT_TRUE = "Yes";
    public static final String DEFAULT_FALSE = "No";

    /**
     * This pattern describes a set of values, comma delimited, with the target value (matching pattern 2) in the list.
     */
    public static final String DEFAULT_TAG_REG_EX_FORMAT = "^(.+,)*(%s)(,.+)*$";

    /**
     * Private constructor for utility class.
     */
    private TagExtractionFunctionFactory() {
    }

    /**
     * Build an extraction function that first matches values that contain the exact tag value and then maps matches
     * to a true value and non matches to a false value.
     *
     * @param tagValue  The value being looked for in the list.
     * @param trueValue  The representation of flags being present in the result.
     * @param falseValue The representation of flags being absent in the result.
     *
     * @return A cascading extraction function that will allow yes/no filtering on a string list dimension value.
     */
    public static List<ExtractionFunction> buildTagExtractionFunction(
            String tagValue,
            String trueValue,
            String falseValue
    ) {
        if ("".equals(tagValue)) {
            throw new IllegalArgumentException("Tag values should not be empty strings.");
        }

        String regExPattern = String.format(DEFAULT_TAG_REG_EX_FORMAT, tagValue);
        Integer index = 2;
        String replaceValue = "";
        ExtractionFunction regularExpressionExtractionFunction = new RegularExpressionExtractionFunction(
                Pattern.compile(regExPattern),
                index,
                replaceValue
        );
        HashMap<String, String> map = new HashMap();
        map.put(tagValue, trueValue);
        MapLookup mapLookup = new MapLookup(map);
        ExtractionFunction lookupExtractionFunction = new LookupExtractionFunction(
                mapLookup,
                false,
                falseValue,
                false,
                false
        );

        return Stream.of(regularExpressionExtractionFunction, lookupExtractionFunction)
                .collect(Collectors.toList());
    }

    /**
     * Build an extraction function that first matches values that contain the exact tag value and then maps matches
     * to a true value and non matches to a false value.
     * Defaults to the "Yes" and "No" values for the flag being present or absent.
     *
     * @param tagValue  The value being looked for in the list.
     *
     * @return A cascading extraction function that will allow yes/no filtering on a string list dimension value.
     */
    public static List<ExtractionFunction> buildTagExtractionFunction(String tagValue) {
        return buildTagExtractionFunction(tagValue, DEFAULT_TRUE, DEFAULT_FALSE);
    }
}
