package net.matlux.bitemporal.parquet;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

public class ParquetBitemporalSnapshot {
    private final Dataset<Row> bitemporalSnap;
    private final ParquetBitemporalUtils parquetBitemporalUtils;

    public ParquetBitemporalSnapshot(SparkSession spark, File parquetFile, ParquetBitemporalUtils parquetBitemporalUtils) {
        this.parquetBitemporalUtils = parquetBitemporalUtils;
        if(parquetFile.exists()){
            this.bitemporalSnap = parquetBitemporalUtils.readParquet(spark, parquetFile.getAbsolutePath());
        } else {
            this.bitemporalSnap = null;
        }


    }

    public Dataset<Row> getLatestData() {
        return parquetBitemporalUtils.getLatestData(bitemporalSnap);
    }
}
