package com.exceeddata.ac.format.dbc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.BitSet;

import com.exceeddata.ac.common.data.record.Record;
import com.exceeddata.ac.common.message.MessageContent;
import com.exceeddata.ac.common.message.MessageDecoder;
import com.exceeddata.ac.common.message.MessageDesc;
import com.exceeddata.ac.common.message.MessageDirection;

public final class DbcUtils {
    private DbcUtils(){}
    
    public static BitSet createSignedLongBitSet() {
        return BitSet.valueOf(new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff});
    }
    
    public static BitSet createUnsignedLongBitSet() {
        return BitSet.valueOf(new byte[] {(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00});
    }
    
    public static BitSet createSignedIntBitSet() {
        return BitSet.valueOf(new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff});
    }
    
    public static BitSet createUnsignedIntBitSet() {
        return BitSet.valueOf(new byte[] {(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00});
    }
    
    public static long bsToLong(final BitSet bs) {
        long value = 0L;
        for (int i = 0; i < 64; ++i) {
          value += bs.get(i) ? (1L << i) : 0L;
        }
        return value;
    }

    public static BigDecimal bsToBigDecimal(final BitSet bs) {
        byte buf [] = new byte[8];
        for (int i = 0; i < 64; ++i) {
            int byte_offset = 7 - i/8;

            buf [byte_offset] += bs.get(i) ? (1L << (i%8)) : 0L;
        }
        BigInteger bi = new BigInteger(1, buf);
        return new BigDecimal( bi.toString());
    }
    
    public static int bsToInt(final BitSet bits) {
        int value = 0;
        for (int i = 0; i < 32; ++i) {
          value += bits.get(i) ? (1 << i) : 0;
        }
        return value;
    }
    
    public static BitSet intToBs(final int val) {
        final BitSet bits = new BitSet();
        int value = val;
        int index = 0;
        while (value != 0) {
            if (value % 2 != 0) {
                bits.set(index);
            }
            ++index;
            value = value >>> 1;
        }
        return bits;
    }
    
    public static BitSet longToBs(final long val) {
        final BitSet bits = new BitSet();
        long value = val;
        int index = 0;
        while (value != 0l) {
            if (value % 2l != 0l) {
                bits.set(index);
            }
            ++index;
            value = value >>> 1;
        }
        return bits;
    }

    public static Record decode(MessageDecoder decoder , Instant time, int channelId, int messageId, byte [] data, Record record  , boolean applyFormula){
        Message msg = new Message(time, channelId, messageId, data);
        decoder.compute(msg,msg ,  record, applyFormula);
        return record;
    }

    static class Message implements MessageContent, MessageDesc {
        private static final long serialVersionUID = -649611365826765678L;
        
        private Instant msg_time;
        private int channelId;
        private long messageId;
        private byte [] data;

        public Message (Instant time, int channelId, long messageId, byte [] data) {
            this.msg_time = time;
            this.channelId = channelId;
            this.messageId = messageId;
            this.data = data;
        }

        @Override
        public int getChannelID() {
            return channelId;
        }

        @Override
        public long getMessageID() {
            return messageId;
        }

        @Override
        public boolean isError() {
            return false;
        }


        @Override
        public Instant getTimeStart() {
            return msg_time;
        }

        @Override
        public long getNanosOffset() {
            //nano offset comparing to getTimeStart.
            //we use one class for meta/message, so offset is always 0
            return 0;
        }

        @Override
        public int getDataLength() {
            return data.length;
        }

        @Override
        public MessageDirection getDirection() {
            return null;
        }

        @Override
        public byte[] getData() {
            return data;
        }

    }

}
