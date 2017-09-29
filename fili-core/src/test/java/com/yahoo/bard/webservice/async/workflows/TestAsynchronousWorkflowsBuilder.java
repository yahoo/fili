// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.async.workflows;

import com.yahoo.bard.webservice.async.jobs.jobrows.JobRow;
import com.yahoo.bard.webservice.async.jobs.stores.ApiJobStore;
import com.yahoo.bard.webservice.async.preresponses.stores.PreResponseStore;
import com.yahoo.bard.webservice.util.Either;
import com.yahoo.bard.webservice.web.PreResponse;

import rx.Observable;
import rx.Observer;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * An extension of DefaultAsynchronousWorkflowsBuilder that behaves exactly like the
 * DefaultAsynchronousWorkflowsBuilder, except it allows outsiders to add an additional subscriber to each workflow.
 * <p>
 * This allows us to perform multi-response tests against the asynchronous workflow in a thread-safe manner, amongst
 * other things.
 */
@Singleton
public class TestAsynchronousWorkflowsBuilder extends DefaultAsynchronousWorkflowsBuilder {

    static final Map<Workflow, Observer> SUBSCRIBERS = new ConcurrentHashMap<>();
    /**
     * A factory for constructing the asynchronous response building workflow.
     *
     * @param apiJobStore A service for storing and requesting job metadata
     * @param preResponseStore A service for storing and requesting query results
     * @param timestampGenerator The clock to use to generate timestamps
     */
    @Inject
    public TestAsynchronousWorkflowsBuilder(
            ApiJobStore apiJobStore,
            PreResponseStore preResponseStore,
            Clock timestampGenerator
    ) {
        super(apiJobStore, preResponseStore, timestampGenerator);
    }

    /**
     * Adds the specified subscriber to the specified workflow.
     *
     * @param workflow  The workflow to add the countdown latch to
     * @param workflowSubscriber  The subscriber that should be added to the specified workflow
     */
    public static void addSubscriber(Workflow workflow, Observer workflowSubscriber) {
        SUBSCRIBERS.put(workflow, workflowSubscriber);
    }

    /**
     * Adds the subscriber (specified in individual method components) to the specified workflow.
     *
     * @param workflow  The workflow to add the countdown latch to
     * @param onNext  onNext method for the observer
     * @param onCompleted  onCompleted method for the observer
     * @param onError  onError method for the observer
     */
    public static void addSubscriber(
            Workflow workflow,
            Consumer<Object> onNext,
            Runnable onCompleted,
            Consumer<Throwable> onError
    ) {
        Observer workflowSubscriber = new Observer() {
            @Override
            public void onNext(Object next) {
                onNext.accept(next);
            }
            @Override
            public void onCompleted() {
                onCompleted.run();
            }
            @Override
            public void onError(Throwable error) {
                onError.accept(error);
            }
        };
        addSubscriber(workflow, workflowSubscriber);
    }

    /**
     * Subscribes the partially-specified observer to the specified workflow.
     * <p>
     * Uses a no-op onCompleted method.
     *
     * @param workflow  The workflow to add the countdown latch to
     * @param onNext  onNext method for the observer
     * @param onError  onError method for the observer
     */
    public static void addSubscriber(Workflow workflow, Consumer<Object> onNext, Consumer<Throwable> onError) {
        addSubscriber(workflow, onNext, () -> { }, onError);
    }

    /**
     * Subscribes the partially-specified observer to the specified workflow.
     * <p>
     * Uses no-op onCompleted and onError methods.
     *
     * @param workflow  The workflow to add the countdown latch to
     * @param onNext  onNext method for the observer
     */
    public static void addSubscriber(Workflow workflow, Consumer<Object> onNext) {
        addSubscriber(workflow, onNext, ignored -> { });
    }

    /**
     * Clears the map of subscribers.
     * This should be invoked in the cleanup of every test that adds latches.
     */
    public static void clearSubscribers() {
        SUBSCRIBERS.clear();
    }

    @Override
    public AsynchronousWorkflows buildAsynchronousWorkflows(
            Observable<PreResponse> preResponseEmitter,
            Observable<Either<PreResponse, JobRow>> payloadEmitter,
            JobRow jobMetadata,
            Function<JobRow, String> jobMetadataSerializer
    ) {
        AsynchronousWorkflows workflows = super.buildAsynchronousWorkflows(
                preResponseEmitter,
                payloadEmitter,
                jobMetadata,
                jobMetadataSerializer
        );

        Map<Workflow, Observable> workflowMap = new ConcurrentHashMap<>();
        workflowMap.put(Workflow.SYNCHRONOUS, workflows.getSynchronousPayload());
        workflowMap.put(Workflow.ASYNCHRONOUS, workflows.getAsynchronousPayload());
        workflowMap.put(Workflow.PRERESPONSE_READY, workflows.getPreResponseReadyNotifications());
        workflowMap.put(Workflow.JOB_MARKED_COMPLETE, workflows.getJobMarkedCompleteNotifications());

        SUBSCRIBERS.entrySet().forEach(it -> workflowMap.get(it.getKey()).subscribe(it.getValue()));

        return workflows;
    }

    /**
     * An enumeration of the possible workflows that Observers can be added to.
     */
    public enum Workflow {
        SYNCHRONOUS,
        ASYNCHRONOUS,
        PRERESPONSE_READY,
        JOB_MARKED_COMPLETE
    }
}
