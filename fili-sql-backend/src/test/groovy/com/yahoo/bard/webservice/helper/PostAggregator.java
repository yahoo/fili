package com.yahoo.bard.webservice.helper;

import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;

import java.util.Arrays;

/**
 * Created by hinterlong on 6/7/17.
 */
public class PostAggregator {

    public static ArithmeticPostAggregation arithmetic(
            ArithmeticPostAggregation.ArithmeticPostAggregationFunction fn,
            PostAggregation... postAggregation
    ) {
        return new ArithmeticPostAggregation(
                "arithmeticOf_" + fn + "_on_" + postAggregation,
                fn,
                Arrays.asList(postAggregation)
        );
    }

    public static FieldAccessorPostAggregation field(Aggregation aggregation) {
        return new FieldAccessorPostAggregation(
                aggregation
        );
    }
}
