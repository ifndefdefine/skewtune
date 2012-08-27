package skewtune.utils;

public final class Average {
    KahanSum sum = new KahanSum();
    int n;
    
    public Average() {}
    
    public synchronized int size() { return n; }

    public synchronized double get() {
        return n > 0 ? sum.value() / n : 0.;
    }
    
    public synchronized void add(double v) {
        sum.add(v);
        ++n;
    }
    
    public synchronized void reset() {
        sum.reset();
        n = 0;
    }
}
