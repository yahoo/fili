// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers.workflow;

import java.util.Optional;

/**
 * Cache Mode represents a strategy for Druid caching.
 */
public interface CacheMode {
    /**
     * Returns true if a particular cache mode is set.
     *
     * @return true if a particular cache mode is set
     */
    Optional<Boolean> isSet();
}
