package com.yahoo.bard.webservice.sql.evaluator;

/**
 * Created by hinterlong on 7/7/17.
 */
public interface Evaluator<T, R> {
    R evaluate(T t);
}
