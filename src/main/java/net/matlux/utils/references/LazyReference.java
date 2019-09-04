package net.matlux.utils.references;

import java.util.function.Supplier;

public class LazyReference<T> {
    private final Supplier<T> supplier;
    private volatile T value;
    //private final Object lock = new Object();

    public LazyReference(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    synchronized public T get() {
        if(value==null){
            value = supplier.get();
        }
        return value;
    }


}
