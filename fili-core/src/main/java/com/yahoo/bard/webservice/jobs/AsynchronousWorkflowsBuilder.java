// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.jobs;

import com.yahoo.bard.webservice.util.Either;
import com.yahoo.bard.webservice.web.PreResponse;

import rx.Observable;

import java.util.function.Function;

/**
 * A SAM that sets up the workflow for shipping query results back to the user, and saving query results for later
 * querying if necessary.
 */
public interface AsynchronousWorkflowsBuilder {

    /**
     * Builds the asynchronous workflows.
     * <p>
     * There are four workflows:
     * <ol>
     *      <li> One that emits the results of the query if the query is synchronous.
     *      <li> One that emits the (serialized) job metadata if the query is asynchronous.
     *      <li> One that emits notifications that the PreResponse is ready to be accessed.
     *      <li> One that emits notifications that the Job has been marked complete.
     * </ol>
     * <p>
     * WARNING: Some implementations (i.e. {@link DefaultAsynchronousWorkflowsBuilder}) may communicate with external
     * resources, or do other potentially expensive operations. If you wish to guarantee that the asynchronous
     * workflows are executed at most once (as opposed to once per subscription), then make sure to pass in instances of
     * {@link rx.observables.ConnectableObservable}.
     *
     * @param preResponseEmitter  The Observable that will eventually emit the results of the backend query
     * @param payloadEmitter  The Observable that will emit the results of the query if the query is
     * synchronous, and the job metadata if the query is asynchronous
     * @param jobMetadata  The query's metadata
     * @param jobMetadataSerializer  A function that serializes a given JobRow into the payload to return to the user
     *
     * @return A bean containing all the asynchronous workflows
     */
    AsynchronousWorkflows buildAsynchronousWorkflows(
            Observable<PreResponse> preResponseEmitter,
            Observable<Either<PreResponse, JobRow>> payloadEmitter,
            JobRow jobMetadata,
            Function<JobRow, String> jobMetadataSerializer
    );
}
