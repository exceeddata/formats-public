package com.exceeddata.ac.format.core;

import java.io.Serializable;
import java.util.Comparator;

public class QueueComparator implements Comparator<FormatQueue>, Serializable { 
    private static final long serialVersionUID = 1L;
    
    public QueueComparator() {}
    
    /** {@inheritDoc} */
    @Override
    public int compare(final FormatQueue q1, final FormatQueue q2) {
        return q1.getTime().compareTo(q2.getTime());
    }
}
