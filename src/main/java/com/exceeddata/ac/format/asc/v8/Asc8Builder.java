package com.exceeddata.ac.format.asc.v8;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.exceeddata.ac.format.asc.AscBuilder;
import com.exceeddata.ac.format.asc.AscUtils;

public class Asc8Builder implements AscBuilder{
    private static final long serialVersionUID = 1L;
    
    private StringBuilder buffer = new StringBuilder(128);
    private boolean canFD = false;
    private boolean v81 = false;
    private Instant startTime = null;
    
    public Asc8Builder(final String protocol, final String version) {
        this.canFD = "CANFD".equalsIgnoreCase(protocol);
        this.v81 = "8.1".compareTo(version) < 0;
    }
    
    @Override
    public List<String> head(
            final Instant startTime,
            final String formatVersion,
            final boolean relativeTime) {
        this.startTime = startTime.minusNanos(startTime.getNano());
        
        final ArrayList<String> headers = new ArrayList<>();
        final String timestr = AscUtils.formatASCDateTime(this.startTime);
        headers.add("date " + timestr);
        headers.add("base hex  timestamps " + (relativeTime ? "relative" : "absolute"));
        headers.add("internal events logged");
        headers.add("// version " + formatVersion);
        headers.add("Begin Triggerblock " + timestr);
        headers.add("   0.000000 Start of measurement");
        return headers;
    }
    
    
    @Override
    public String row(
            final int channel,
            final long messageID,
            final long nanoOffset,
            final boolean tx,
            final boolean xFrame,
            final byte[] content,
            final boolean nanosPrecision) {
        return canFD
                ? rowCANFD(channel, messageID, nanoOffset, tx, xFrame, content, nanosPrecision)
                : rowCAN  (channel, messageID, nanoOffset, tx, xFrame, content, nanosPrecision);
    }
    
    @Override
    public Instant getStartTime() {
        return startTime;
    }
    
    public String rowCAN(
            final int channel,
            final long messageID,
            final long nanoOffset,
            final boolean tx,
            final boolean xFrame,
            final byte[] content,
            final boolean nanosPrecision) {
        final int length = content.length;
        final String soffset = nanosPrecision 
                ? String.format("%14.9f", nanoOffset / 1000000000d) 
                : String.format("%11.6f", Math.round(nanoOffset / 1000d) / 1000000d);
        final String schannel = channel <= 9 ? channel + " " : String.valueOf(channel);
        final String slength = " d " + String.valueOf(length); //max 8
        final String smessage = String.format("%-15s", Long.toHexString(messageID).toUpperCase() + (xFrame ? "x" : ""));
        
        buffer.setLength(0);
        buffer.append(soffset + " " + schannel + " " + smessage + " " + (tx ? "Tx" : "Rx") + " " + slength);
        
        for (int i = 0; i < length; ++i) {
            buffer.append(String.format(" %02X", content[i]).toUpperCase());
        }
        
        buffer.append("    ID = " + String.valueOf(messageID));
        if (xFrame) {
            buffer.append("x");
        }
        
        return buffer.toString();
    }
    
    public String rowCANFD(
            final int channel,
            final long messageID,
            final long nanoOffset,
            final boolean tx,
            final boolean xFrame,
            final byte[] content,
            final boolean nanosPrecision) {
        final int length = content.length;
        final int padlen = AscUtils.padDLC(length);
        final String soffset = nanosPrecision 
                ? String.format("%14.9f", nanoOffset / 1000000000d) 
                : String.format("%11.6f", Math.round(nanoOffset / 1000d) / 1000000d);
        final String schannel = channel <= 9 ? channel + " " : String.valueOf(channel);
        final String slength = AscUtils.buildDLC(padlen) + " " + String.valueOf(padlen); //max 64
        final String smessage = String.format("%-15s", Long.toHexString(messageID).toUpperCase() + (xFrame ? "x" : ""));
        
        buffer.setLength(0);
        if (v81) {
            buffer.append(soffset + " CANFD " + schannel + " " + (tx ? "Tx" : "Rx") + " " + smessage + "    0 0 " + slength);
        } else {
            buffer.append(soffset + " CANFD " + schannel + " " + (tx ? "Tx" : "Rx") + " " + smessage + "    0 0 d " + slength);
        }
        
        for (int i = 0; i < length; ++i) {
            buffer.append(String.format(" %02X", content[i]).toUpperCase());
        }
        if (padlen > length) {
            for (int i = padlen - length; i > 0; --i) {
                buffer.append(" 00");
            }
        }
        
        if (v81) {
            buffer.append(AscUtils.sampleExtInfo(tx));
        } else {
            buffer.append("    ID = " + String.valueOf(messageID));
        }
        
        return buffer.toString();
    }
}
