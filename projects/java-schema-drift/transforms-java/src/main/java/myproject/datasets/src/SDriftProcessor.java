package myproject.datasets.src;


import com.palantir.transforms.lang.java.api.Compute;
import com.palantir.transforms.lang.java.api.Input;
import com.palantir.transforms.lang.java.api.Output;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.collect.AbstractIterator;
import com.google.common.io.CharSource;
import com.google.common.io.Closeables;

import com.google.common.io.ByteStreams;

import com.palantir.transforms.lang.java.api.FoundryInput;
import com.palantir.transforms.lang.java.api.FoundryOutput;
import com.palantir.transforms.lang.java.api.ReadOnlyLogicalFileSystem;
import com.palantir.transforms.lang.java.api.WriteOnlyLogicalFileSystem;
import com.palantir.util.syntacticpath.Paths;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import com.google.common.io.CharSource;
import com.google.common.io.Closeables;

import com.palantir.spark.binarystream.data.PortableFile;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.util.TaskCompletionListener;
import java.util.Arrays;

import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;


/**
* This is an example high-level Transform intended for automatic registration.
*/
public final class SDriftProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(SDriftProcessor.class);

    private static final class InputStreamCharSource extends CharSource {
        private final Reader inputStream;

        private InputStreamCharSource(InputStream inputStream) {
            this.inputStream = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        }

        @Override
        public Reader openStream() throws IOException {
            return inputStream;
        }

        @SuppressWarnings("MustBeClosedChecker")
        Iterator<String> getLineIterator() {
            try {
                return super.lines().iterator();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                if (TaskContext.get() != null) {
                    // If running in Spark, close the stream when the task is finished.
                    TaskContext.get().addTaskCompletionListener((TaskCompletionListener) context -> close());
                } else {
                    close();
                }
            }
        }

        private void close() {
            Closeables.closeQuietly(inputStream);
        }
    }

    public Seq<PortableFile> getPortableFileData(List<PortableFile> portableFilesList) {
        return JavaConverters.asScalaBufferConverter(portableFilesList).asScala().toSeq();
    }

    public Set<String> schemaConsolidator(List<Dataset<Row>> listDS) {
        Set<String> masterSchema = new HashSet<>();
        for(Dataset<Row> ds : listDS) {
          masterSchema.addAll(Arrays.asList(ds.columns()));
        }
        return masterSchema;
    }

    @Compute
    public void compute(@Input("/BP/IST-IG-Commodity-Risk-Sandbox/Datasets/Ingestion/DL010-PNL_Detail") FoundryInput input, 
                        @Output("/BP/IST-IG-Commodity-Risk-Sandbox/Datasets/Ingestion/POC_schema_Drift/test_05_v2") FoundryOutput output) throws IOException {


        List<Dataset<Row>> masterDS = new ArrayList<>();
        List<Dataset<Row>> masterDSComputed = new ArrayList<>();
        List<Dataset<Row>> masterDSSorted = new ArrayList<>();

       Dataset<PortableFile> files = input.asFiles().getFileSystem().filesAsDataset();



    List<Dataset<String>> csvDataset = new ArrayList<>();
      for(PortableFile portableFile : files.collectAsList()) {
          List<PortableFile> localPortableFilesList = new ArrayList<>();
          localPortableFilesList.add(portableFile);
          Seq<PortableFile> localPortableFilesSeq = getPortableFileData(localPortableFilesList);
          Dataset<PortableFile> portableFileDS = files.sparkSession().createDataset(localPortableFilesSeq, 
                  Encoders.kryo(PortableFile.class));
          csvDataset.add(portableFileDS.flatMap((FlatMapFunction<PortableFile, String>) portableFile2 ->
                    portableFile2.convertToIterator(inputStream -> {
                        return new InputStreamCharSource(inputStream).getLineIterator();

         }), Encoders.STRING()));
      }
                Dataset<Row> datasetMaster = null;
                for(Dataset<String> ds : csvDataset) {
                  Dataset<Row> dataset = files
                        .sparkSession()
                        .read()
                        .option("inferSchema", "true")
                        .option("header", "true")
                        .csv(ds);
                   masterDS.add(dataset);
                    
                }

           Set<String> masterSchema = schemaConsolidator(masterDS);

        for(Dataset<Row> ds2 : masterDS) {
           for(String col : masterSchema) {
                if(! Arrays.asList(ds2.columns()).contains(col)) {
                    ds2 = ds2.withColumn(col, functions.lit(null));
                }
            }
            masterDSComputed.add(ds2);
        }


        String fields[] = masterSchema.toArray(new String[0]);

        Arrays.sort(fields);

        for(Dataset<Row> ds : masterDSComputed) {
            ds = ds.select(getColumns(fields));
            masterDSSorted.add(ds);
        }

        int position = 0;
        // Dataset<Row> ds_res = null;
        for(Dataset<Row> ds : masterDSSorted) {
            position++;
            if(position == 1) {
                datasetMaster = ds;
            } else {
                datasetMaster = datasetMaster.union(ds);
            }
        }

        datasetMaster = datasetMaster.select(FieldVersionMapping.mapping(FieldVersionMapping.VERSION));

        // datasetMaster = ComputeTyped.process_df(datasetMaster);
        output.getDataFrameWriter(datasetMaster).write();

    }

    public static Seq<Column> getColumns(String[] fields) {
        List<Column> lists = new ArrayList<>();
        for(String field : fields) {
            lists.add(functions.col(field));
        }
        return JavaConverters.asScalaBufferConverter(lists).asScala().toSeq();
    }

}
