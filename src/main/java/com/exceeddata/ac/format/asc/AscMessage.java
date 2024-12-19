package com.exceeddata.ac.format.asc;

import java.math.BigDecimal;
import java.util.List;

import com.exceeddata.ac.common.message.MessageContent;
import com.exceeddata.ac.common.message.MessageDirection;
import com.exceeddata.ac.common.util.SpaceSimpleParser;

public class AscMessage implements MessageContent {
    private static final long serialVersionUID = 1L;
    
    private long nanosOffset; //nanosOffset
    private int channelID; //channel
    private boolean error;
    private String flags; // CAN Rx and Tx
    private int dataLength; //length
    private long messageID; //id
    private byte[] data;
    
    protected AscMessage() {
        this(false);
    }
    
    protected AscMessage(final boolean canFd) {
        this(0l, 0, 0, false, "rx", 0, new byte[canFd ? 64 : 8]);
    }
    
    public AscMessage(
            final long nanosOffset,
            final int channelID,
            final long messageID,
            final boolean error,
            final String flags,
            final int dataLength,
            final byte[] data) {
        this.nanosOffset = nanosOffset;
        this.channelID = channelID;
        this.messageID = messageID;
        this.error = error;
        this.flags = flags;
        this.dataLength = dataLength;
        this.data = data;
    }
    
    @Override
    public long getNanosOffset() {
        return nanosOffset;
    }
    
    public void setNanosOffset(final long nanosOffset) {
        this.nanosOffset = nanosOffset;
    }
    
    @Override
    public int getChannelID() {
        return channelID;
    }
    
    public void setChannelID(final int channelID) {
        this.channelID = channelID;
    }
    
    @Override
    public long getMessageID() {
        return messageID;
    }
    
    public void setMessageID(final long messageID) {
        this.messageID = messageID;
    }
    
    @Override
    public boolean isError() {
        return error;
    }
    
    public void setError(final boolean error) {
        this.error = error;
    }
    
    public String getFlags() {
        return flags;
    }
    
    public void setFlags(final String flags) {
        this.flags = flags;
    }
    
    @Override
    public int getDataLength() {
        return dataLength;
    }
    
    public void setDataLength(final int dataLength) {
        this.dataLength = dataLength;
    }
    
    @Override
    public byte[] getData() {
        return data;
    }
    
    public void setData(final byte[] data) {
        this.data = data;
    }
    
    @Override
    public MessageDirection getDirection()  {
        return "rx".equals(flags.toLowerCase()) ?  MessageDirection.RX: MessageDirection.TX;
    }
    
    /**
     * Parse absolute timestamp ASC line. The line needs to be trimmed before calling.
     * 
     * @param line the ASC line
     * @param hexbase true or false
     * @return ASC Message
     */
    public static AscMessage fromString(final String line, final boolean hexbase) {
        final List<String> pieces = SpaceSimpleParser.split(line);
        final int size = pieces.size();
        if (size < 7) {
            return null;
        }

        try {
            final long nanosOffset = new BigDecimal(pieces.get(0)).movePointRight(9).longValue();
            final boolean canfdFormat = pieces.get(1).toLowerCase().startsWith("can");
            final int busID = Integer.parseInt(pieces.get(canfdFormat ? 2 : 1));
            final String flags = pieces.get(3);
            
            String msgID = pieces.get(canfdFormat ? 4 : 2);
            char m = msgID.charAt(msgID.length() - 1);
            if (m == 'x' || m == 'X') {
                msgID = msgID.substring(0, msgID.length() - 1);
            }
            final long messageID = Long.parseLong(msgID, 16);
            final int dataLength = Integer.parseInt(pieces.get(canfdFormat ? 8 : 5));
            final List<String> remaining = pieces.subList(canfdFormat ? 9 : 6,  size);
            if (remaining.size() < dataLength) { //invalid number of bytes
                return null;
            }
            
            return fromDecoded(hexbase, nanosOffset, busID, messageID, flags, dataLength, remaining);
        } catch (NumberFormatException e) {
        }
        
        return null;
    }
    
    /**
     * Parse relative timestamp ASC format line. The line needs to be trimmed before parsing.
     * 
     * @param line the ASC line
     * @param hexbase true or false
     * @param lastOffset the accumulative last offset
     * @return ASC Message
     */
    public static AscMessage fromString(final String line, final boolean hexbase, final long lastOffset) {
        final List<String> pieces = SpaceSimpleParser.split(line);
        final int size = pieces.size();
        if (size < 7) {
            return null;
        }
        
        final boolean canfdFormat = pieces.get(1).toLowerCase().startsWith("can");
        if (canfdFormat && size < 10) {
            return null;
        }

        try {
            final long nanosOffset = new BigDecimal(pieces.get(0)).movePointRight(9).longValue() + lastOffset;
            final int busID = Integer.parseInt(pieces.get(canfdFormat ? 2 : 1));
            final String flags = pieces.get(3);
            
            String msgID = pieces.get(canfdFormat ? 4 : 2);
            char m = msgID.charAt(msgID.length() - 1);
            if (m == 'x' || m == 'X') {
                msgID = msgID.substring(0, msgID.length() - 1);
            }
            final long messageID = Long.parseLong(msgID, 16);
            final int dataLength = Integer.parseInt(pieces.get(canfdFormat ? 8 : 5));
            final List<String> remaining = pieces.subList(canfdFormat ? 9 : 6,  size);
            if (remaining.size() < dataLength) { //invalid number of bytes
                return null;
            }
            
            return fromDecoded(hexbase, nanosOffset, busID, messageID, flags, dataLength, remaining);
        } catch (NumberFormatException e) {
        }
        
        return null;
    }

    private static AscMessage fromDecoded(
            final boolean hexbase,
            final long nanosOffset, 
            final int busID, 
            final long messageID,
            final String flags,
            final int dataLength,
            final List<String> remaining) throws NumberFormatException {
        final byte[] data = new byte[dataLength];
        if (hexbase) {
            for (int i = 0; i < dataLength; ++i) {
                final String s = remaining.get(i);
                if (s.length() != 2) { //invalid byte hex length
                    return null;
                }
                data[i] = (byte) ((Character.digit(s.charAt(0), 16) << 4) + Character.digit(s.charAt(1), 16));
            }
        } else {
            for (int i = 0; i < dataLength; ++i) {
                data[i] = (byte) Integer.parseInt(remaining.get(i));
            }
        }
        
        return new AscMessage(nanosOffset, busID, messageID, false, flags, dataLength, data);
    }
}
