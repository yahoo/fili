// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Incubating is a marker annotation used to indicate that the marked piece of code is subject to significant API
 * changes, including removal, and should NOT be depended upon until the {@link Incubating} annotation is removed.
 * Code marked incubating is intended to be code that is in a transitive state, and was exposed to the public api before
 * it was ready to be. This may be due to a partial feature being merged to the main branch and releasing alongside a
 * critical change that must be released before the partial feature should be completed. If code marked incubating is
 * released, immediate effort must be made to publish the final version of the relevant API as soon as possible so the
 * Incubating annotation can be removed.
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.CONSTRUCTOR, ElementType.FIELD, ElementType.METHOD, ElementType.PACKAGE, ElementType.TYPE})
public @interface Incubating {
}
