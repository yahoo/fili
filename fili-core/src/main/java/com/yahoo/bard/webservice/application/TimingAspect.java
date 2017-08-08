// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import com.yahoo.bard.webservice.logging.RequestLog;
import com.yahoo.bard.webservice.logging.TimedPhase;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
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
    @Pointcut("execution(* *(..)) && @annotation(com.yahoo.bard.webservice.application.RequestLogTimed)")
    public void inTimeableMethod() {

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
    @Around("inTimeableMethod()")
    public <T> T time(ProceedingJoinPoint joinPoint) throws Throwable {
        String timerName = getTimerName(joinPoint);

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
     * @param joinPoint  The joinpoint in the program to continue at.
     *
     * @return the name to use for the timer.
     */
    private String getTimerName(ProceedingJoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
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
