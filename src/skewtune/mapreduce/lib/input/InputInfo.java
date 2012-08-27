package skewtune.mapreduce.lib.input;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InputInfo {
    private FileSplit split;
    private long pos;
   
    private static final ThreadLocal<InputInfo> _info = new ThreadLocal<InputInfo>();
    
    // create only through getInstance
    private InputInfo() {}
    
    public FileSplit getSplit() { return split; }
    public long getPosition() { return pos; }
    
    void setSplit(FileSplit split) {
        this.split = split;
    }
    void setPosition(long pos) { this.pos = pos; }
    
    public static InputInfo getInstance() {
        InputInfo info = _info.get();
        if ( info == null ) {
            info = new InputInfo();
            _info.set(info);
        }
        return info;
    }
}
