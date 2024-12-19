package com.exceeddata.ac.format.dbc;

import java.util.BitSet;

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
}
