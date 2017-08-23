// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import javax.ws.rs.container.ContainerRequestContext;

/**
 * A functional interface which allows the logic of resolving response format customizable. It has one method that
 * takes the format name string from URI and a ContainerRequestContext object from which the header Accept field is
 * accessible.
 */
public interface ResponseFormatResolver {
    /**
     * Resolve desirable format from URI and ContainerRequestContext.
     *
     * @param format  The format String from URI
     * @param containerRequestContext  ContainerRequestContext object that contains request related information
     * @return A resolved format decided by the function
     */
    String accept(String format, ContainerRequestContext containerRequestContext);
}
