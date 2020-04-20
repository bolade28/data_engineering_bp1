package myproject.datasets.util;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BP_ConversionUtil {
    public static final String COL_UNCOFORMED_UOM = "Unconformed_Curve_Delta_UOM";
    public static final String COL_COFORMED_UOM = "Curve_Delta_UOM";
    public static final String COL_UNCOFORMED_VALUE = "Curve_Delta_UOM";
    public static final String COL_COFORMED_VALUE = "Curve_Delta";
    public static final String COL_CONVERSION_RATE = "Conversion_Rate";

    public static Dataset<Row> addConversionRate(
        Dataset<Row> df,
        Dataset<Row> df_uom_conversions,
        String colDelta,
        String colSourceUnit,
        String colTargetUnit,        
        String colConversionRate,
        boolean throwExceptionIfNotFound
    )
    {                
        df_uom_conversions = functions.broadcast(df_uom_conversions
            .withColumnRenamed("From_Unit", "Uom_Conversions_From_Unit")
            .withColumnRenamed("To_Unit", "Uom_Conversions_To_Unit")
            .withColumnRenamed("Conversion", "Uom_Conversions_Conversion")
        );

        df = df.join(df_uom_conversions, df.col(colSourceUnit).equalTo(df_uom_conversions.col("Uom_Conversions_From_Unit"))
                .and( df.col(colTargetUnit).equalTo(df_uom_conversions.col("Uom_Conversions_To_Unit"))), "left");
        
        df = df.withColumn(colConversionRate, 
            functions.coalesce(
                functions.when(df.col(colSourceUnit).equalTo(df.col(colTargetUnit)), functions.lit(1.0)),
                df.col("Uom_Conversions_Conversion")
            )
        );

        Dataset<Row> df_missing_rates = df.filter(df.col(colConversionRate).isNull()).select(df.col(colSourceUnit), df.col(colTargetUnit)).distinct();
        if (df_missing_rates.count()>0) /// TODO improve
        {
            // throw exception with all missing pairs
            StringBuilder strMissingPairs = new StringBuilder();
            
            // TODO
            List<Row> missingCombinations = df_missing_rates.collectAsList();
            for(Row r : missingCombinations)
            {
                strMissingPairs.append("("+r.getString(0)+", "+r.getString(1)+")\n");
            }
            Logger log = LoggerFactory.getLogger(BP_ConversionUtil.class);
            log.info("Missing conversions for "+strMissingPairs);
            if (throwExceptionIfNotFound)
                throw new RuntimeException("missing conversion rates for some UOM combinations "+strMissingPairs);
        }
        
        df = df.drop("Uom_Conversions_From_Unit")
            .drop( "Uom_Conversions_To_Unit")
            .drop( "Uom_Conversions_Conversion")
            ;

        return df;
    }

    public static Dataset<Row> addConformedUnit(
        Dataset<Row> df,
        Dataset<Row> df_ConformedUnits,
        String colUom,
        String colConformedUom        
    )
    {
        df_ConformedUnits = functions.broadcast(df_ConformedUnits
            .withColumnRenamed("From_Unit", "Conformed_Units_From_Unit")
            .withColumnRenamed("Conformed_Unit", "Conformed_Units_Conformed_Unit")
            .filter(df_ConformedUnits.col("BU").equalTo(functions.lit("LNG"))).drop("BU")
        );
        
        df = df.join(df_ConformedUnits,  df.col(colUom).equalTo(df_ConformedUnits.col("Conformed_Units_From_Unit")), "left" );
        df = df.withColumn(colConformedUom,
            functions.coalesce(df.col("Conformed_Units_Conformed_Unit"), df.col(colUom))
        );
        df  = df.drop("Conformed_Units_From_Unit").drop("Conformed_Units_Conformed_Unit");
        return df;                
    }    


}
