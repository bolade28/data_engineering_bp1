/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 */

package myproject.datasets;

//import com.palantir.transforms.lang.java.api.Compute;
//import com.palantir.transforms.lang.java.api.Input;
//import com.palantir.transforms.lang.java.api.Output;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//
///**
// * This is an example high-level Transform intended for automatic registration.
// */
//public final class HighLevelAutoTransform {
//
//    // The class for an automatically registered Transform contains the compute
//    // function and information about the input/output datasets.
//    // Automatic registration requires "@Input" and "@Output" annotations.
//    @Compute
//    @Output("/path/to/output/dataset")
//    public Dataset<Row> myComputeFunction(@Input("/path/to/input/dataset") Dataset<Row> myInput) {
//        // The compute function for a high-level Transform returns an output of type "Dataset<Row>".
//        return myInput.limit(10);
//    }
//}
