// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;

import java.lang.reflect.Method;

/**
 * Defines an Aspect for timing methods using the {@link RequestLog} via the
 * {@link com.yahoo.bard.webservice.application.RequestLogTimed} annotation.
 */
@Aspect
public class TimingAspect {

    /**
     * Defines a pointcut to the execution of methods annotated with {@link RequestLogTimed}.
     */
    @Pointcut("execution(@com.yahoo.bard.webservice.application.RequestLogTimed * *(..))")
    public void inTimeableMethod() {

    }

    /**
     * Defines a pointcut to the execution of methods annotated with {@link RequestLogTimed} returning a
     * {@link javax.ws.rs.core.Response}.
     */
    @Pointcut("execution(@com.yahoo.bard.webservice.application.RequestLogTimed javax.ws.rs.core.Response *(..))")
    public void startTimerForResponse() {

    }

    /**
     * Defines a pointcut to within methods annotated with {@link RequestLogTimed} returning a
     * {@link javax.ws.rs.core.Response}.
     */
    @Pointcut("withincode(@com.yahoo.bard.webservice.application.RequestLogTimed javax.ws.rs.core.Response *(..))")
    public void withinResponseCall() {

    }

    /**
     * Defines a pointcut to calls of {@link javax.ws.rs.core.Response#status} including chained calls.
     */
    @Pointcut("call(* javax.ws.rs.core.Response.status (*))")
    public void startBuildingResponse() {

    }

    /**
     * Defines a pointcut to methods which return a {@link javax.ws.rs.core.Response} and have calls to {@code
     * Response.status(...).*}.
     */
    @Pointcut("withinResponseCall() && startBuildingResponse()")
    public void endTimerForResponse() {

    }

    // CHECKSTYLE:OFF

    /**
     * Defines a pointcut to the execution of methods annotated with {@link RequestLogTimed} and containing an
     * {@link javax.ws.rs.container.AsyncResponse}.
     */
    @Pointcut("execution(@com.yahoo.bard.webservice.application.RequestLogTimed * *(..,@javax.ws.rs.container.Suspended (*),..))")
    public void startTimerForAsyncResponse() {

    }

    /**
     * Defines a pointcut to within methods annotated with {@link RequestLogTimed} and containing an
     * {@link javax.ws.rs.container.AsyncResponse}.
     */
    @Pointcut("withincode(@com.yahoo.bard.webservice.application.RequestLogTimed * *(..,@javax.ws.rs.container.Suspended (*),..))")
    public void withinAsyncResponseCall() {

    }

    // CHECKSTYLE:ON

    /**
     * Defines a pointcut to calls of {@link javax.ws.rs.container.AsyncResponse#resume}.
     */
    @Pointcut("call(boolean javax.ws.rs.container.AsyncResponse.resume(..))")
    public void resumeAsyncResponse() {

    }

    /**
     * Defines a pointcut to calls of {@link com.yahoo.bard.webservice.web.handlers.DataRequestHandler#handleRequest}.
     */
    @Pointcut("call(boolean com.yahoo.bard.webservice.web.handlers.DataRequestHandler.handleRequest(..))")
    public void handleRequest() {

    }

    /**
     * Defines a pointcut to resuming async responses/handling requests inside of methods with an AsyncResponse and
     * annotated for timing.
     */
    @Pointcut("withinAsyncResponseCall() && (resumeAsyncResponse() || handleRequest())")
    public void endTimerForAsyncResponse() {

    }

    /**
     * Starts a timer for a methods which are advised by {@link #startTimerForAsyncResponse()}.
     *
     * @param joinPoint  The joinpoint in the program to continue at.
     *
     * @throws Throwable for any exceptions that occur.
     */
    @Before("startTimerForAsyncResponse()")
    public void startTimerAsyncResponse(JoinPoint joinPoint) throws Throwable {
        startTimer(joinPoint);
    }

    /**
     * Starts a timer for a methods which are advised by {@link #startTimerForResponse()}.
     *
     * @param joinPoint  The joinpoint in the program to continue at.
     *
     * @throws Throwable for any exceptions that occur.
     */
    @Before("startTimerForResponse()")
    public void startTimerResponse(JoinPoint joinPoint) throws Throwable {
        startTimer(joinPoint);
    }

    /**
     * Starts a timer using the {@link RequestLog} with the name found from {@link #getTimerName(Method)}.
     *
     * @param joinPoint  The joinpoint in the program to continue at.
     *
     * @throws Throwable for any exceptions that occur.
     */
    public void startTimer(JoinPoint joinPoint) throws Throwable {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        String timerName = getTimerName(method);

        RequestLog.startTiming(timerName);
    }

    /**
     * Ends a timer for a method which are advised by {@link #endTimerForAsyncResponse()}.
     *
     * @param enclosingJoinPointStaticPart  The outer joinpoint in the program to continue at (i.e. the method).
     *
     * @throws Throwable for any exceptions that occur.
     */
    @Before("endTimerForAsyncResponse()")
    public void endTimerAsyncResponse(JoinPoint.EnclosingStaticPart enclosingJoinPointStaticPart) throws Throwable {
        endTimerBeforeMethodEnd(enclosingJoinPointStaticPart);
    }

    /**
     * Ends a timer for a method which are advised by {@link #endTimerForResponse()}.
     *
     * @param enclosingJoinPointStaticPart  The outer joinpoint in the program to continue at (i.e. the method).
     *
     * @throws Throwable for any exceptions that occur.
     */
    @Before("endTimerForResponse()")
    public void endTimerResponse(JoinPoint.EnclosingStaticPart enclosingJoinPointStaticPart) throws Throwable {
        endTimerBeforeMethodEnd(enclosingJoinPointStaticPart);
    }

    /**
     * Ends a timer for a method enclosing the joinpoint.
     *
     * @param enclosingJoinPointStaticPart  The outer joinpoint in the program to continue at (i.e. the method).
     *
     * @throws Throwable for any exceptions that occur.
     */
    public void endTimerBeforeMethodEnd(JoinPoint.EnclosingStaticPart enclosingJoinPointStaticPart) throws Throwable {
        MethodSignature signature = (MethodSignature) enclosingJoinPointStaticPart.getSignature();
        Method method = signature.getMethod();
        String timerName = getTimerName(method);

        RequestLog.stopTiming(timerName);
    }

    /**
     * Wraps the {@link RequestLogTimed} method in a timer using the {@link RequestLog}.
     *
     * @param joinPoint  The joinpoint in the program to continue at.
     * @param <T>  The return type of the method being called.
     *
     * @return the value from the method.
     *
     * @throws Throwable for any exceptions that occur.
     */
    @Around("inTimeableMethod() && !startTimerForAsyncResponse() && !startTimerForResponse()")
    public <T> T timeMethod(ProceedingJoinPoint joinPoint) throws Throwable {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        String timerName = getTimerName(method);

        T returnItem;
        try (TimedPhase timedPhase = RequestLog.startTiming(timerName)) {
            returnItem = (T) joinPoint.proceed();
        }

        return returnItem;
    }

    /**
     * Finds what name to be used for the timer. First checks {@link RequestLogTimed#name()}, then
     * the meta values from {@link RequestLogTimed#metaValue()}.
     *
     * @param method  The method in the program being timed.
     *
     * @return the name to use for the timer.
     */
    private String getTimerName(Method method) {
        RequestLogTimed requestLogAnnotation = method.getAnnotation(RequestLogTimed.class);

        if (!requestLogAnnotation.name().isEmpty()) {
            return requestLogAnnotation.name();
        }

        switch (requestLogAnnotation.metaValue()) {
            case CLASS:
                return method.getDeclaringClass().getSimpleName();
            case METHOD:
            default: // it will never go straight to default so fall through from METHOD.
                return method.getDeclaringClass().getName() + "." + method.getName();
        }

    }
}
