// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query;

/**
 * Common interface for Druid Query classes.
 *
 * @param <Q> class that extends DruidMetadataQuery
 */
public interface DruidMetadataQuery<Q extends DruidMetadataQuery<? super Q>> extends DruidQuery<Q> { }
