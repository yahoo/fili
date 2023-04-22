// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import com.yahoo.bard.webservice.async.jobs.jobrows.JobRow
import com.yahoo.bard.webservice.data.dimension.impl.FlagFromTagDimension
import com.yahoo.bard.webservice.druid.druid.model.AggregationExternalNode
import com.yahoo.bard.webservice.druid.druid.model.PostAggregationExternalNode
import com.yahoo.bard.webservice.druid.druid.model.WithMetricFieldInternalNode
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectWriter

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Days
import org.joda.time.Interval

import spock.lang.IgnoreIf
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Timeout
import spock.lang.Unroll

import java.lang.reflect.Modifier
import java.util.regex.Pattern

@Timeout(15)
class ClassScannerSpec extends Specification {

    static ClassScanner classScanner = new ClassScanner("com.yahoo.bard")
    @Shared
    DateTimeZone currentTZ

    /**
     * Anonymous classes take the form of ParentClassName$1, this will match such class names
     */
    static String anonymousClassPattern = /.*\$\d+/

    def setupSpec() {
        currentTZ = DateTimeZone.getDefault()
        DateTimeZone.setDefault(DateTimeZone.UTC)

        classScanner.putInArgumentValueCache(
                [new LongSumAggregation("name", "fieldName"),
                 new DateTime(20000),
                 new Interval(1, 2),
                 Days.days(1),
                 DateTimeZone.UTC
                ])
        classScanner.putInArgumentValueCache(ObjectWriter.class, new ObjectMapper().writer())
    }

    def shutdownSpec() {
        DateTimeZone.setDefault(currentTZ)
    }

    /**
     * Load all available classes under com.yahoo.bard.* that declare given method
     *
     * @param method declared method to find
     * @param parameterTypes parameter types for declared method
     * @return classes that match to be tested
     */
    static List<Class> getClassesDeclaring(String method, Class<?>... parameterTypes) {
        List<Class> classList= []

        // the .equals from these declaring classes will not be tested
        Set ignoreDeclaringClasses = [
                Object,
                AbstractSet,
                AbstractList,
                AbstractMap,
                AbstractMap.SimpleEntry,
                LinkedHashMap,
                JobRow,
                FlagFromTagDimension,
                // Test classes with "special" equals logic
                AggregationExternalNode,
                PostAggregationExternalNode,
                WithMetricFieldInternalNode,
        ]

        for (Class cls : classScanner.classes) {
            // test only classes that override equals
            if (Modifier.isAbstract(cls.modifiers) || cls.enum || Pattern.matches(anonymousClassPattern, cls.name)) {
                continue
            }

            // check if this class or any superclass (except ignore list) implements .equals
            try {
                if (ignoreDeclaringClasses.contains(cls.getMethod(method, parameterTypes).declaringClass)) {
                    continue
                }
            } catch (NoSuchMethodException | UnsupportedOperationException ignored) {
                continue
            }

            classList.add(cls)
        }

        return classList
    }

    /**
     * This tests the 'equals' method across all methods that have implemented it (see getClassesDeclaring).
     * <p>
     * If this test fails for you you may need to update ClassScanner to handle your special argument types,
     * add values to the class scanner value cache above, or add your class to the ignored list in
     * getClassesDeclaring.
     * <p>
     * This method is ignored if no declaring classes will trigger the where block.
     * This is an issue because downstream developers may want to defensively deploy subclasses of this class to
     * ensure that any legitimate class scanner targets are picked up when discovered and appropriate.
     * <p>
     * This method also allows for supplying dependencies for the class under test. To do so, the method will call
     * <tt>Map&lt;Class, Object&gt; supplyDependencies()</tt> on a Spec file for the class under test (eg.
     * <tt>ClassUnderTestSpec</tt> if such a Spec exists with that method.
     */
    @IgnoreIf({ClassScannerSpec.getClassesDeclaring("equals", Object.class).empty})
    @Unroll
    def "test equals for #className"() {
        setup:
        try {
            // Allow class specs to define well formed dependencies
            Map<Class, Object> dependencies = Class.forName("${cls.name}Spec").newInstance().supplyDependencies()
            dependencies.each {
                classScanner.putInArgumentValueCache(it.key, it.value)
            }
        } catch( Exception ignore ) {
        }

        expect:
        // Create test object with default values
        Object obj1 = classScanner.constructObject( cls, ClassScanner.Args.VALUES )
        obj1.equals(obj1)
        ! obj1.equals(null)
        ! obj1.equals(new Object())

        // Create second test object with same values
        Object obj2 = classScanner.constructObject( cls, ClassScanner.Args.VALUES  )
        System.identityHashCode(obj1) != System.identityHashCode(obj2)
        obj1.hashCode() == obj2.hashCode()
        obj1 == obj2

        // attempt to create third object with nulls and if able, call functions, test for not equals
        Object objNulls
        try {
            objNulls = classScanner.constructObject(cls, ClassScanner.Args.NULLS)
        } catch (InstantiationException ignored) {
            objNulls = null
        }

        // make sure these calls run and do not fail with NullPointerException
        if (objNulls != null) {
            objNulls.equals(null)
            objNulls.equals(new Object())
            ! obj1.equals(objNulls) || obj1.hashCode() == objNulls.hashCode()
            ! objNulls.equals(obj1) || obj1.hashCode() == objNulls.hashCode()
            objNulls.hashCode()
        }

        where:
        cls << getClassesDeclaring("equals", Object.class)
        className = cls.simpleName
    }

    @Unroll
    def "test toString for #className runs"() {
        expect:
        Object obj
        try {
            // Create test object with NULL values
            obj = classScanner.constructObject(cls, ClassScanner.Args.NULLS)
        } catch (InstantiationException ignored) {
            // else create test object with default values.  If fails here consider making Mock argument in setupSpec
            obj = classScanner.constructObject(cls, ClassScanner.Args.VALUES)
        }

        // make sure toString does not blow up
        obj.toString() != null

        where:
        cls << getClassesDeclaring("toString")
        className = cls.simpleName
    }
}
