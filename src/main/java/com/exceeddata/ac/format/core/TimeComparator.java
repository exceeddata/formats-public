package com.exceeddata.ac.format.core;

import java.io.Serializable;
import java.util.Comparator;

import com.exceeddata.ac.common.data.record.Record;

public class TimeComparator implements Comparator<Record>, Serializable { 
    private static final long serialVersionUID = 1L;
    
    public TimeComparator() {}
    
    /** {@inheritDoc} */
    @Override
    public int compare(final Record r1, final Record r2) {
        return r1.dataAt(0).compareTo(r2.dataAt(0));
    }
}
