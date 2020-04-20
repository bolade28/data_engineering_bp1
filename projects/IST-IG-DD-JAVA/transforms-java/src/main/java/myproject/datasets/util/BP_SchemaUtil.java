package myproject.datasets.util;

import org.apache.spark.sql.functions;
import scala.collection.JavaConverters;   
import scala.collection.Iterator;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import scala.collection.Seq;

public class BP_SchemaUtil
{
    public static Seq<Column> getColNamesOrdered(StructType st)
    {
        Iterator<StructField> it = st.iterator();
        List<Column> lists = new ArrayList<>();
        while(it.hasNext()) {
            lists.add(functions.col(it.next().name()));
        }
        return JavaConverters.asScalaBufferConverter(lists).asScala().toSeq();
    }
}