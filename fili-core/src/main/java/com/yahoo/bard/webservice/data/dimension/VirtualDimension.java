// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;


/**
 * Marker interface for dimension instances which don't have associated fact columns.  These can be used to create
 * output columns generated during response processing, such as for pseudodimensions.
 */
public interface VirtualDimension {
}
