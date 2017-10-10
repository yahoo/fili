// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation triggers the {@link TimingAspect} aspect and allows entire methods to be timed.
 *
 * <p/>
 * Given a method like this:
 * <pre><code>
 *     &#64;RequestLogTimed(name = "StringConcat")
 *     public String concat(String name) {
 *         return "Prepended " + name;
 *     }
 * </code></pre>
 * <p/>
 *
 * A timer will be started for {@code StringConcat} anytime {@code #concat(String)} is called.
 *
 * <p/>
 * Cases
 * <ol>
 *     <li>The method returns a javax.ws.rs.core.Response</li>
 *      <span>Starts a timer before method and stops it before calling #handleRequest or AsyncResponse#resume</span>
 *     <li>The method has a javax.ws.rs.container.AsyncResponse</li>
 *      <span>Starts a timer before method and stops it before building the Response</span>
 *     <li>The method falls back to just starting/stopping a timer around execution</li>
 *      <span>Starts a timer before method then executes the method then stops it.</span>
 * </ol>
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

    /**
     * This tells the timer to use a {@link MetaValue} which by default is the name of the current method.
     *
     * @return the meta value to be used for the timer name.
     */
    MetaValue metaValue() default MetaValue.METHOD;

    /**
     * A meta value to be used for the timer, i.e. the name of the method or the name of the class.
     * <p/>
     * Meta values are evaluated as below, using {@link Object#toString()} as an example:
     * <pre><code>
     * {@link MetaValue#METHOD} -> "java.lang.Object.toString"
     * {@link MetaValue#CLASS} -> "Object"
     * </code></pre>
     * <p/>
     */
    enum MetaValue {
        METHOD,
        CLASS
    }
}
