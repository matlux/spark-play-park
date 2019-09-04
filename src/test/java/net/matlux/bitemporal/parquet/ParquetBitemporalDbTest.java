package net.matlux.bitemporal.parquet;

import net.matlux.utils.spark.SparkSessionManager;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.junit.Assert.*;

public class ParquetBitemporalDbTest {

    private final Logger logger = LoggerFactory.getLogger(ParquetBitemporalDbTest.class);
    private File parquetFile;
    private SparkSessionManager sparkSessionManager;
    private ParquetBitemporalDb parquetBitemporal;
    private SparkSession spark;
    private ParquetBitemporalUtils parquetBitemporalUtils;

    @Before
    public void setUp() throws Exception {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("UnitTests");
        sparkConf.setMaster("local[*]");
        if(parquetFile.exists()) {
            FileUtils.deleteDirectory(parquetFile);
        }

        sparkSessionManager = new SparkSessionManager(sparkConf);
        spark = sparkSessionManager.getSparkSession();
        parquetBitemporal = new ParquetBitemporalDb(spark,
                parquetFile.getAbsolutePath(),
                "OBSERVATION_DATE",
                "AS_OF",
                "timeseries");
        parquetBitemporalUtils = parquetBitemporal.getParquetBitemporalUtils();
    }

    @After
    public void tearDown() throws Exception {
        parquetBitemporal.close();
        sparkSessionManager.close();
    }

}