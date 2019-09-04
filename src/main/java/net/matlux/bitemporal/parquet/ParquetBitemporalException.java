package net.matlux.bitemporal.parquet;

import java.io.FileNotFoundException;

public class ParquetBitemporalException extends Exception {
    public ParquetBitemporalException(String message, Throwable e) {
        super(message,e);
    }
}
