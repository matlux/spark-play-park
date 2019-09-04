package net.matlux.utils.spark;

import net.matlux.utils.references.LazyReference;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

public class SparkSessionManager implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(SparkSessionManager.class);

    private final SparkConf sparkConf;
    private final LazyReference<SparkSession> sparkSessionLazyReference;

    public SparkSessionManager(SparkConf sparkConf) {
        this.sparkConf = sparkConf;
        this.sparkSessionLazyReference = new LazyReference<SparkSession>(() -> {
            logger.info("creating Spark Session");
            Stream.of(sparkConf.getAll()).forEach(
                    tuple -> logger.info("spark config: {} = {}",tuple._1(),tuple._2())
            );
            return SparkSession.builder().config(sparkConf).getOrCreate();
        });

    }

    public SparkSession getSparkSession() {
        return sparkSessionLazyReference.get();
    }

    @Override
    public void close() throws Exception {
        sparkSessionLazyReference.get().close();
    }
}
