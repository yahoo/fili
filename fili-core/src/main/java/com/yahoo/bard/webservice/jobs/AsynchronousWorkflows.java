// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.jobs;

import com.yahoo.bard.webservice.web.PreResponse;

import rx.Observable;

/**
 * A bean containing the Observables for each of the message flows needed to support asynchronous processing:
 * <ol>
 * <li> The flow that emits the results to be sent to the user if they are ready within the asynchronous timeout.
 * <li> The flow that emits the metadata payload to be sent to the user if the data response is not ready within the
 * asynchronous timeout.
 * <li> The flow that stores the JobRow in the ApiJobStore, the PreResponse in the PreResponseStore, and notifies all
 * subscribers when the PreResponse has been successfully stored
 * <li> The flow that updates the JobRow with the Success (or Error) status once the PreResponse has been stored
 * successfully
 * </ol>
 * <p>
 * Users may access the results of each workflow by extracting the appropriate Observables from the bean, and
 * subscribing to them.
 */
public class AsynchronousWorkflows {

    private final Observable<PreResponse> queryResultsPayload;
    private final Observable<String> jobMetadataPayload;
    private final Observable<String> preResponseReadyNotifications;
    private final Observable<JobRow> jobMarkedCompleteNotifications;

    /**
     * Builds a bean containing all the message flows that support asynchronous processing.
     *
     * @param queryResultsPayload  Emits query results to send to the user
     * @param jobMetadataPayload  Emits job metadata to send to the user
     * @param preResponseReadyNotifications  Emits notifications that the query results have been stored in the
     * {@link PreResponseStore}
     * @param jobMarkedCompleteNotifications  Emits notifications that the JobRow has been updated with success or error
     * upon completion of the query
     */
    public AsynchronousWorkflows(
            Observable<PreResponse> queryResultsPayload,
            Observable<String> jobMetadataPayload,
            Observable<String> preResponseReadyNotifications,
            Observable<JobRow> jobMarkedCompleteNotifications
    ) {
        this.queryResultsPayload = queryResultsPayload;
        this.jobMetadataPayload = jobMetadataPayload;
        this.preResponseReadyNotifications = preResponseReadyNotifications;
        this.jobMarkedCompleteNotifications = jobMarkedCompleteNotifications;
    }

    /**
     * Returns an Observable that emits the query results to send to the user.
     *
     * @return An Observable that emits the query results to send to the user
     */
    public Observable<PreResponse> getSynchronousPayload() {
        return queryResultsPayload;
    }

    /**
     * Returns an Observable that emits the job metadata to send to the user.
     *
     * @return an Observable that emits the job metadata to send to the user
     */
    public Observable<String> getAsynchronousPayload() {
        return jobMetadataPayload;
    }

    /**
     *  Returns an Observable that emits notifications that the query results have been stored in the
     *  {@link PreResponseStore}.
     *
     * @return an Observable that emits notifications that the query results have been stored in the
     * {@link PreResponseStore}
     */
    public Observable<String> getPreResponseReadyNotifications() {
        return preResponseReadyNotifications;
    }

    /**
     * Returns an Observable that emits notifications that the JobRow has been updated.
     * <p>
     *  Typically, a JobRow will be updated with some sort of notification (i.e. a Status value) indicating that the
     *  query has completed, either successfully or with an error.
     *
     * @return an Observable that emits notifications that the JobRow has been updated with success or error
     * upon completion of the query
     */
    public Observable<JobRow> getJobMarkedCompleteNotifications() {
        return jobMarkedCompleteNotifications;
    }
}
