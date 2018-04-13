// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.handlers.workflow;

import com.yahoo.bard.webservice.web.handlers.DataRequestHandler;

/**
 * Request workflow provider builds a request processing chain for handing druid data requests.
 */
@FunctionalInterface
public interface RequestWorkflowProvider {

    /**
     * Construct a workflow instance with a starting point request handler.
     * <p>
     * The workflow should be acyclic, moving a request along until a response is submitted or delegated to an
     * asynchronous response callback.
     *
     * @return The data request handler at the start of the workflow chain
     */
    DataRequestHandler buildWorkflow();
}
