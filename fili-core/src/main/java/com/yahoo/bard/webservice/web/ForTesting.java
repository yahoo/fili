// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Intended to signify that a constructor is defined only for testing purposes.
 */
@Target({ElementType.CONSTRUCTOR})
@Retention(RetentionPolicy.SOURCE)
public @interface ForTesting { }
