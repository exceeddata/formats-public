package com.exceeddata.ac.format.asc;

import java.time.Instant;

import com.exceeddata.ac.common.message.MessageDesc;

public class AscMeta implements MessageDesc {
    private static final long serialVersionUID = 1L;
    
    private Instant timeStart = null;
    private boolean hexbase = true;
    private boolean relative = false;
    
    public Instant getTimeStart() {
        return timeStart;
    }
    
    public AscMeta setTimeStart(final Instant timeStart) {
        this.timeStart = timeStart;
        return this;
    }
    
    public boolean getHexBase() {
        return hexbase;
    }
    
    public AscMeta setHexBase(final boolean hexbase) {
        this.hexbase = hexbase;
        return this;
    }
    
    public boolean getRelative() {
        return relative;
    }
    
    public AscMeta setRelative(final boolean relative) {
        this.relative = relative;
        return this;
    }
}
