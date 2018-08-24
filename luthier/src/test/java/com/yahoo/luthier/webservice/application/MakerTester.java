// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.application;

import com.yahoo.bard.webservice.data.time.ZonelessTimeGrain;
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation.ArithmeticPostAggregationFunction;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is used for testing findConstructor and parseParam functions in MetricMakerDictionary.
 */
public class MakerTester {

    int intPara;
    boolean booleanParam;
    char charParam;

    String stringParam;
    ZonelessTimeGrain timeParam;
    ArithmeticPostAggregationFunction functionParam;

    List listParam;
    Set setParam;
    Map<String, Integer> mapStringToIntParam;
    Map<Integer, List> mapIntToListParam;

    /**
     * Constructor.
     *
     * @param intParam int parameter
     */
    public MakerTester(int intParam) {
        this.intPara = intPara;
    }

    /**
     * Constructor.
     *
     * @param intParam int parameter
     * @param stringParam string parameter
     */
    public MakerTester(int intParam, String stringParam) {
        this.intPara = intParam;
        this.stringParam = stringParam;
    }

    /**
     * Constructor.
     *
     * @param charParam char parameter
     * @param functionParam ArithmeticPostAggregationFunction parameter
     */
    public MakerTester(char charParam, ArithmeticPostAggregationFunction functionParam) {
        this.charParam = charParam;
        this.functionParam = functionParam;
    }

    /**
     * Constructor.
     *
     * @param timeParam time parameter
     * @param booleanParam boolean parameter
     */
    public MakerTester(ZonelessTimeGrain timeParam, boolean booleanParam) {
        this.timeParam = timeParam;
        this.booleanParam = booleanParam;
    }

    /**
     * Constructor.
     *
     * @param listParam list parameter
     */
    public MakerTester(List listParam) {
        this.listParam = listParam;
    }

    /**
     * Constructor.
     *
     * @param listParam list parameter
     * @param mapStringToIntParam map parameter
     */
    public MakerTester(List listParam, Map mapStringToIntParam) {
        this.listParam = listParam;
        this.mapStringToIntParam = mapStringToIntParam;
    }

    /**
     * Constructor.
     *
     * @param mapIntToSetParam map parameter
     */
    public MakerTester(Map mapIntToSetParam) {
        this.mapIntToListParam = mapIntToSetParam;
    }

    /**
     * Constructor.
     *
     * @param setParam set parameter
     */
    public MakerTester(Set setParam) {
        this.setParam = setParam;
    }
}
