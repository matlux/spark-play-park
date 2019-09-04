package net.matlux.bitemporal.parquet;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.function.BiFunction;

public class ParquetBitemporalDb implements AutoCloseable {

    private final SparkSession spark;
    private final String parquetFilePath;
    private final String dataAsAtCol;
    private final String dataAsOfCol;
    private final String timeseriesCol;
    private final File parquetFile;
    private final FileLock lock;
    private final FileChannel channel;
    private final File fileLock;
    private final ParquetBitemporalUtils parquetBitemporalUtils;

    public ParquetBitemporalDb(SparkSession spark, String parquetFilePath, String dataAsAtCol, String dataAsOfCol, String timeseriesCol)
        throws ParquetBitemporalException {
        this.spark = spark;
        this.parquetFilePath = parquetFilePath;
        this.parquetFile = new File(parquetFilePath);
        this.dataAsAtCol = dataAsAtCol;
        this.dataAsOfCol = dataAsOfCol;
        this.timeseriesCol = timeseriesCol;
        this.parquetBitemporalUtils = new ParquetBitemporalUtils(dataAsAtCol,dataAsOfCol,timeseriesCol);
        try{
            this.fileLock = new File(parquetFilePath + ".lock");
            try{
                this.channel = new RandomAccessFile(fileLock,"rw").getChannel();
                this.lock = channel.tryLock();
            } catch (FileNotFoundException e) {
                throw new ParquetBitemporalException("Couldn't lock the Parquet lock file under: " + parquetFilePath + ".lock",e);
            } catch (IOException e) {
                throw new ParquetBitemporalException("Couldn't lock the Parquet lock file under: " + parquetFilePath + ".lock",e);
            }
        } catch (OverlappingFileLockException e) {
            throw new ParquetBitemporalException("Couldn't lock the Parquet lock file under: " + parquetFilePath + ".lock",e);
        }

    }

    public ParquetBitemporalSnapshot getLatestBitemporalSnapshot() {
        return new ParquetBitemporalSnapshot(spark,parquetFile,parquetBitemporalUtils);
    }

    public void append(Dataset<Row> timeSeries2append, BiFunction<ParquetBitemporalSnapshot,Dataset<Row>, Dataset<Row>> fn) {
        if(parquetFile.exists()) {
            ParquetBitemporalSnapshot bitemporalSnapshot = getLatestBitemporalSnapshot();
            Dataset<Row> diff = fn.apply(bitemporalSnapshot,timeSeries2append);
            parquetBitemporalUtils.writeParquet(diff,parquetFilePath);
        }
    }

    public void close() throws Exception {
        try{
            lock.release();
            channel.close();
            fileLock.delete();
        } catch(Exception e) {
            throw new ParquetBitemporalException("couldn't close the parquet lock file under: "+ parquetFilePath,e);
        }
    }

    public ParquetBitemporalUtils getParquetBitemporalUtils() {
        return parquetBitemporalUtils;
    }
}
