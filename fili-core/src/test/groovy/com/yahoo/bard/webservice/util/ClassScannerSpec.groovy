// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import com.yahoo.bard.webservice.async.jobs.jobrows.JobRow

import org.joda.time.DateTimeZone

import spock.lang.IgnoreIf
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Timeout
import spock.lang.Unroll

import java.lang.reflect.Modifier
import java.util.regex.Pattern

@Timeout(5)
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
        DateTimeZone.setDefault(DateTimeZone.UTC);
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
                JobRow
        ]

        for ( Class cls : classScanner.getClasses() ) {
            // test only classes that override equals
            if (Modifier.isAbstract(cls.getModifiers())) {
                continue
            }
            if (cls.isEnum()) {
                continue
            }
            if (Pattern.matches(anonymousClassPattern, cls.name)) {
                continue
            }

            /* check if this class or any superclass (except ignore list) implements .equals */
            try {
                Class declaringClass = cls.getMethod(method,parameterTypes).getDeclaringClass()
                if ( ignoreDeclaringClasses.contains( declaringClass ) ) {
                    continue
                }
            } catch ( NoSuchMethodException | UnsupportedOperationException e ) {
                continue
            }

            classList.add(cls)
        }

        return classList
    }

    /**
     * This method is ignored if no declaring classes will trigger the where block.
     * This is an issue because downstream developers may want to defensively deploy subclasses of this class to
     * ensure that any legitimate class scanner targets are picked up when discovered and appropriate.
     */
    @IgnoreIf({ ClassScannerSpec.getClassesDeclaring("equals", Object.class).empty })
    @Unroll
    def "test equals #cls.simpleName"() {
        expect:
        // Create test object with default values
        Object obj1 = classScanner.constructObject( cls, ClassScanner.Args.VALUES )
        obj1.equals(obj1) == true
        obj1.equals(null) == false
        obj1.equals(new Object()) == false

        // Create second test object with same values
        Object obj2 = classScanner.constructObject( cls, ClassScanner.Args.VALUES  )
        System.identityHashCode(obj1) != System.identityHashCode(obj2)
        obj1.equals(obj2) == true
        obj1.hashCode() == obj2.hashCode()

        // attempt to create third object with nulls and if able, call functions, test for not equals
        Object objNulls
        try {
            objNulls = classScanner.constructObject( cls, ClassScanner.Args.NULLS  )
        } catch ( InstantiationException e ) {
            objNulls = null
        }

        // make sure these calls run and do not fail with NullPointerException
        if ( objNulls != null ) {
            objNulls.equals(null)
            objNulls.equals(new Object())
            ! obj1.equals(objNulls) || obj1.hashCode() == objNulls.hashCode()
            ! objNulls.equals(obj1) || obj1.hashCode() == objNulls.hashCode()
            objNulls.hashCode()
        }

        where:
        cls << getClassesDeclaring("equals", Object.class)
    }

    @Unroll
    def "test toString #cls.simpleName runs"() {
        expect:

        Object obj
        try {
            // Create test object with NULL values
            obj = classScanner.constructObject( cls, ClassScanner.Args.NULLS  )
        } catch ( InstantiationException e ) {
            // else create test object with default values.  If fails here consider making Mock argument in setupSpec
            obj = classScanner.constructObject( cls, ClassScanner.Args.VALUES  )
        }

        // make sure toString does not blow up
        obj.toString() != null

        where:
        cls << getClassesDeclaring("toString" )
    }
}
