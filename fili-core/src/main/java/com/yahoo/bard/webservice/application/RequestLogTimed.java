// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation triggers the {@link TimingAspect} aspect and allows entire methods to be timed.
 * <p/>
 * Given a method like this:
 * <pre><code>
 *    {@literal @}RequestLogTimed(name = "StringConcat")
 *     public String concat(String name) {
 *         return "Prepended " + name;
 *     }
 * </code></pre>
 * <p/>
 * A timer will be started for {@code StringConcat} anytime {@code #concat(String)} is called.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RequestLogTimed {
    /**
     * The name to be used for the timer. If this is empty (which it is by default) it will check the
     * {@link MetaValue} from {@link #metaValue()}.
     *
     * @return the timer name.
     */
    String name() default "";

    // every annotation is on a method so it has to have a method, but it could be static

    /**
     * This tells the timer to use a {@link MetaValue} which by default is the name of the current method.
     *
     * @return the meta value to be used for the timer name.
     */
    MetaValue metaValue() default MetaValue.METHOD;

    /**
     * A meta value to be used for the timer, i.e. the name of the method or the name of the class.
     */
    enum MetaValue {
        METHOD,
        CLASS
    }
}
