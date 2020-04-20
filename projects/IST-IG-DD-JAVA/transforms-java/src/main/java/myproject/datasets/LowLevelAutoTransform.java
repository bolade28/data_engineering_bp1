/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 */

package myproject.datasets;

//import com.palantir.transforms.lang.java.api.Compute;
//import com.palantir.transforms.lang.java.api.FoundryInput;
//import com.palantir.transforms.lang.java.api.FoundryOutput;
//import com.palantir.transforms.lang.java.api.Input;
//import com.palantir.transforms.lang.java.api.Output;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//
///**
// * This is an example low-level Transform intended for automatic registration.
// */
//public final class LowLevelAutoTransform {
//
//    // The class for an automatically registered Transform contains the compute
//    // function and information about the input/output datasets.
//    // Automatic registration requires "@Input" and "@Output" annotations.
//    @Compute
//    public void myComputeFunction(
//            @Input("/path/to/input/dataset") FoundryInput myInput,
//            @Output("/path/to/output/dataset") FoundryOutput myOutput) {
//        Dataset<Row> limited = myInput.asDataFrame().read().limit(10);
//        // The compute function for a low-level Transform writes to the output dataset(s),
//        // instead of returning the output(s).
//        myOutput.getDataFrameWriter(limited).write();
//    }
//}
