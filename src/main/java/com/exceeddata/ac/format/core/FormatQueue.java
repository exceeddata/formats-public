package com.exceeddata.ac.format.core;

import java.io.IOException;

import com.exceeddata.ac.common.data.record.Record;

public interface FormatQueue {
    
    public Record pollAndAdd() throws IOException;
    
    public Record peek();
    
    public boolean isEmpty();
    
    public Long getTime();
}
