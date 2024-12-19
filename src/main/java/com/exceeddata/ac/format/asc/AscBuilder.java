package com.exceeddata.ac.format.asc;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;

public interface AscBuilder extends Serializable {

    public List<String> head(Instant startTime, String formatVersion, boolean relativeTime);
    
    public String row(int channel, long messageID, long nanoOffset, boolean tx, boolean xFrame, byte[] content, boolean nanosPrecision);
    
    /**
     * Return a sanitized start time.
     * 
     * @return Instant
     */
    public Instant getStartTime();
}
