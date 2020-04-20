package myproject.datasets.util;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.Vector;
import org.apache.spark.sql.Column;
import scala.collection.Iterator;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.functions;
import scala.collection.JavaConverters;
import scala.collection.Seq;


public class BP_DatasetUtils {
public static StructType buildSchema(Class clazz) throws IllegalAccessException {
		Field[] fields = clazz.getDeclaredFields();
		Vector<StructField> structFields = new Vector<StructField>();
		for (Field f : fields) {
			String fieldName = f.getName();
			Class fieldType = f.getType();
			try {
				DataType sparkDataType;
				if (fieldType == String.class) {
					sparkDataType = DataTypes.StringType;
				} else if (fieldType == Double.class || fieldType == double.class) {
					sparkDataType = DataTypes.DoubleType;
				} else if (fieldType == Date.class || fieldType == java.sql.Date.class) {
					sparkDataType = DataTypes.DateType;
				} else if (fieldType == java.sql.Timestamp.class) {
					sparkDataType = DataTypes.TimestampType;
				} else if (fieldType == Long.class || fieldType == long.class) {
					sparkDataType = DataTypes.LongType;
				} else if (fieldType == Integer.class || fieldType == int.class) {
					sparkDataType = DataTypes.IntegerType;
				} else if (fieldType == Boolean.class || fieldType == boolean.class) {
					sparkDataType = DataTypes.BooleanType;
				} else {
					throw new RuntimeException("no conversion for field " + fieldName + " data type " + fieldType);
				}
				StructField newField = new StructField(fieldName, sparkDataType, true, Metadata.empty());
				structFields.add(newField);
			} catch (Exception e) {
				throw new RuntimeException("Error convering " + fieldName + " data type " + fieldType + e.getMessage(),
						e);
			}
		}
		StructType schema = new StructType(structFields.toArray(new StructField[0]));
		SparkContext context;
		return schema;
	}
	
	public static Row getValuesAsRow(Object obj) throws IllegalAccessException
	{
		Field[] fields=obj.getClass().getDeclaredFields();
		Object[] values = new Object[fields.length];
		new Date(System.currentTimeMillis());
		for (int i=0; i<fields.length;i++)
			values[i] = fields[i].get(obj);
		return RowFactory.create(values);
	}
	
	public static Object getRowAsModelObject(Row row, Class modelClass) throws IllegalAccessException, InstantiationException	{
		Field[] fields = modelClass.getDeclaredFields();
		Object obj = modelClass.newInstance(); 
		StructType schema = row.schema();
		for (Field field : fields) {
			int schemaFieldIndex = schema.fieldIndex(field.getName()); 
			Object value = row.get(schemaFieldIndex);
			field.set(obj, value); 			
		}
		return obj;
	}	

	    public static Seq<Column> getColumns(StructType st) {
        Iterator<StructField> it = st.iterator();
        List<Column> lists = new ArrayList<>();
        while(it.hasNext()) {
            lists.add(functions.col(it.next().name()));
        }
        return JavaConverters.asScalaBufferConverter(lists).asScala().toSeq();
    }

}
