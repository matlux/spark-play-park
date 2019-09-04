package net.matlux.bitemporal.parquet;

import org.apache.commons.lang3.builder.DiffResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class ParquetBitemporalUtils {

    private final String dataAsAtCol;
    private final String dataAsOfCol;
    private final String timeseriesCol;

    public ParquetBitemporalUtils(String dataAsAtCol, String dataAsOfCol,String timeseriesCol) {
        this.dataAsAtCol = dataAsAtCol;
        this.dataAsOfCol = dataAsOfCol;
        this.timeseriesCol = timeseriesCol;
    }

    public Dataset<Row> getLatestData(Dataset<Row> df) {
        return df.groupBy("ADDITIONAL_CATEGORY",dataAsAtCol)
                .agg(last(timeseriesCol))
                .withColumnRenamed("last(" + timeseriesCol + ", false)",timeseriesCol)
                .drop(dataAsOfCol);
    }

    public Dataset<Row> getDataAsOf(Dataset<Row> df, String exDate) {
        return df.filter(col(dataAsOfCol).lt(to_date(lit(exDate))))
                .groupBy("ADDITIONAL_CATEGORY",dataAsAtCol)
                .agg(last(timeseriesCol))
                .withColumnRenamed("last(" + timeseriesCol + ", false)",timeseriesCol)
                .drop(dataAsOfCol);
    }
    public Dataset<Row> readParquet(SparkSession spark, String path) {
        return spark.read().parquet(path).select("ADDITIONAL_CATEGORY",dataAsAtCol,timeseriesCol,dataAsOfCol).sort(dataAsOfCol);
    }
    public void writeParquet(Dataset<Row> df, String path) {
        df.withColumn("month",month(col(dataAsAtCol)))
                .withColumn("year",year(col(dataAsOfCol)))
                .withColumn("day", dayofmonth(col(dataAsOfCol)));
    }

    public Dataset<Row> addExtractionTime(Dataset<Row> df, String exDate) {
        return df.withColumn(dataAsOfCol,to_date(lit(exDate)));
    }
    public Dataset<Row> prepareBitemporalMarketData2Parquet(ParquetBitemporalSnapshot bitemporalSnap, Dataset<Row> timeSeries2append) {
        Dataset<Row> latestData = bitemporalSnap.getLatestData();
        Dataset<Row> timeSeries2ToAppend = timeSeries2append.except(latestData);
        return this.addExtractionTime(timeSeries2ToAppend,this.getLatestDate(timeSeries2append,dataAsAtCol).toString());
    }

    private String getLatestDate(Dataset<Row> df, String dataAsAtCol) {
        return df.select(dataAsOfCol).distinct().sort(desc(dataAsOfCol)).first().getAs(0).toString();
    }


}
