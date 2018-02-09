// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application.metadataViews;

import java.util.function.BiFunction;

import javax.ws.rs.container.ContainerRequestContext;

public interface MetadataViewProvider<T> extends BiFunction<ContainerRequestContext, T, Object> {

}

