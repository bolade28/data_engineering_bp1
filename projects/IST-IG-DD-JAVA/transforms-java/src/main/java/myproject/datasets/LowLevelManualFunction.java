/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 */

package myproject.datasets;

//import com.palantir.transforms.lang.java.api.Compute;
//import com.palantir.transforms.lang.java.api.FoundryInput;
//import com.palantir.transforms.lang.java.api.FoundryOutput;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//
///**
// * This is an example compute function for a low-level Transform intended for manual registration.
// */
//public final class LowLevelManualFunction {
//
//    // The class for a manually registered Transform contains just the compute function.
//    @Compute
//    public void myComputeFunction(FoundryInput myInput, FoundryOutput myOutput) {
//        Dataset<Row> limited = myInput.asDataFrame().read().limit(10);
//       // The compute function for a low-level Transform writes to the output dataset(s),
//       // instead of returning the output(s).
//        myOutput.getDataFrameWriter(limited).write();
//    }
//}
