// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An Either is a bean that contains one of two possible values, referred to generically as Left or Right.
 * <p>
 * An Either is _not_ a pair. It does not contain two values. It only contains one value. However, the type of the value
 * could either be the left type, or the right type.
 * <p>
 * A poor facsimile of the Either monad in Haskell that should make all Haskellers cry a little.
 *
 * @param <L>  The type of the Left value
 * @param <R>  The type of the Right value
 */
public class Either<L, R> {
    private static final Logger LOG = LoggerFactory.getLogger(Either.class);

    private final L left;
    private final R right;
    private final boolean isLeft;

    /**
     * Builds an object that is either of type L, or type R.
     *
     * @param left  The left value, this should be null if isLeft is false
     * @param right  The right value, this should be null if isLeft is true
     * @param isLeft  Whether or not this is a right or left value
     */
    private Either(L left, R right, boolean isLeft) {
        this.left = left;
        this.right = right;
        this.isLeft = isLeft;
    }

    /**
     * Constructs an Either containing a left value.
     *
     * @param left  The left value to store in the either
     * @param <L>  The type of the left value
     * @param <R>  The type of the right value
     *
     * @return An Either wrapping the left value
     */
    public static <L, R> Either<L, R> left(L left) {
        return new Either<>(left, null, true);
    }

    /**
     * Constructs an Either containing a right value.
     *
     * @param right  The right value to store in the either
     * @param <L>  The type of the left value
     * @param <R>  The type of the right value
     *
     * @return An Either wrapping the right value
     */
    public static <L, R> Either<L, R> right(R right) {
        return new Either<>(null, right, false);
    }

    /**
     * Returns whether or not this Either represents a Left value.
     *
     * @return True if this Either is a Left value, false otherwise
     */
    public boolean isLeft() {
        return isLeft;
    }

    /**
     * Returns the Left value wrapped in this Either.
     *
     * @return The Left value wrapped in this Either
     *
     * @throws UnsupportedOperationException If this Either wraps a Right value instead
     */
    public L getLeft() {
        if (!isLeft()) {
            LOG.error(String.format(ErrorMessageFormat.EITHER_ERROR_LEFT_OF_RIGHT.getLoggingFormat(), this));
            throw new UnsupportedOperationException(ErrorMessageFormat.EITHER_ERROR_LEFT_OF_RIGHT.format(this));
        }
        return left;
    }

    /**
     * Returns whether or not this Either represents a Right value.
     *
     * @return True if this Either is a Right value, false otherwise
     */
    public boolean isRight() {
        return !isLeft;
    }

    /**
     * Returns the Right value wrapped in this Either.
     *
     * @return The Right value wrapped in this Either
     *
     * @throws UnsupportedOperationException If this Either wraps a Left value instead
     */
    public R getRight() {
        if (isLeft()) {
            LOG.error(String.format(ErrorMessageFormat.EITHER_ERROR_RIGHT_OF_LEFT.getLoggingFormat(), this));
            throw new UnsupportedOperationException(ErrorMessageFormat.EITHER_ERROR_RIGHT_OF_LEFT.format(this));
        }
        return right;
    }
}
