// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation.DefaultPostAggregationType

import com.yahoo.bard.webservice.druid.model.DefaultQueryType
import com.yahoo.bard.webservice.druid.model.filter.Filter

import spock.lang.Specification
import spock.lang.Unroll

import java.lang.invoke.MethodHandleImpl.BindCaller.T

class EnumUtilsSpec extends Specification {

    final static enum TestEnum {
        ONE,
        TWO,
        THIS_IS_A_TEST, // tests short one character word
        A_FIRST,
        LAST_A,
        X_Both_Y,
        E_I_E_I_O,
    }

    @Unroll
    def "check enumJsonName #a is #b"() {
        expect:
        EnumUtils.enumJsonName(a) == b

        where:
        a                                       || b
        DAY                                     || "day"
        DefaultQueryType.GROUP_BY               || "groupBy"
        Filter.DefaultFilterType.SELECTOR       || "selector"
        DefaultPostAggregationType.FIELD_ACCESS || "fieldAccess"
        TestEnum.THIS_IS_A_TEST                 || "thisIsATest"
        TestEnum.A_FIRST                        || "aFirst"
        TestEnum.LAST_A                         || "lastA"
        TestEnum.X_Both_Y                       || "xBothY"
        TestEnum.E_I_E_I_O                      || "eIEIO"
    }

    def "check forKey"(){
        Map<String, T> mapping = ["1":TestEnum.ONE, "2":TestEnum.TWO]

        expect:
        TestEnum.ONE == EnumUtils.forKey("1", mapping, TestEnum.class)
        TestEnum.TWO == EnumUtils.forKey("2", mapping, TestEnum.class)
    }

    def "check missing forKey"(){
        Map<String, T> mapping = ["1":TestEnum.ONE, "2":TestEnum.TWO]

        when:
        EnumUtils.forKey("THIS_IS_A_TEST", mapping, TestEnum.class)

        then:
        IllegalArgumentException e = thrown()
        e.getMessage().toString() == """Not an alternate key for class com.yahoo.bard.webservice.util.EnumUtilsSpec\$TestEnum: THIS_IS_A_TEST"""
    }
}
