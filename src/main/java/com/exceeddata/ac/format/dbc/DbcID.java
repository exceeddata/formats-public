package com.exceeddata.ac.format.dbc;

import java.io.Serializable;

public class DbcID implements Serializable, Comparable<DbcID> {
    private static final long serialVersionUID = 1L;

    private int channelID = 0;
    private long messageID = 0l;
    
    public DbcID(final int channelID, final long messageID) {
        this.channelID = channelID;
        this.messageID = messageID;
    }
    
    @Override
    public DbcID clone() {
        return new DbcID(this.channelID, this.messageID);
    }
    
    public int getChannelID() {
        return channelID;
    }
    
    public void setChannelID(final int channelID) {
        this.channelID = channelID;
    }
    
    public long getMessageID() {
        return messageID;
    }
    
    public void setMessageID(final int messageID) {
        this.messageID = messageID;
    }
    
    @Override
    public int hashCode() {
        return (int) (channelID * 129 + messageID);
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof DbcID) {
            final DbcID dobj = (DbcID) obj;
            return dobj.messageID == this.messageID && dobj.channelID == this.channelID;
        }
        return false;
    }

    @Override
    public int compareTo(DbcID dobj) {
        if (channelID > dobj.channelID) {
            return 1;
        } else if (channelID == dobj.channelID) {
            if (messageID > dobj.messageID) {
                return 1;
            } else if (messageID == dobj.messageID) {
                return 0;
            } else {
                return -1;
            }
        } else {
            return -1;
        }
    }
}
