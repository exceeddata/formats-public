package com.exceeddata.ac.format.dbc;

import java.math.BigDecimal;
import java.util.BitSet;

import com.exceeddata.ac.common.data.record.Hashing;
import com.exceeddata.ac.common.data.typedata.LongData;
import com.exceeddata.ac.common.data.typedata.TypeData;
import com.exceeddata.ac.common.exception.EngineException;

public class DbcAttributeLong implements DbcAttribute {
    private static final long serialVersionUID = 1L;
    
    private String name = null;
    private String unit = null;
    private int nameHash = 0;
    
    private int startBit = 0; //depending on order, start bit is LSB for Intel, MSB for Motorola
    private int length = 1;
    private DbcByteOrder order = DbcByteOrder.MOTOROLA;
    private boolean signed = false;
    private long factor = 0l;
    private long offset = 0l;
    private long minValue = 0l;
    private long maxValue = 0l;
    
    //internal user variables
    private int startByte = 0;
    private int startByteBit = 0;
    private int endIntelByte = 0;
    private int endIntelByteBit = 0;
    private int lengthMinusOne = 0;
    private LongData zeroOffsetData = LongData.ZERO;
    private LongData oneOffsetData = LongData.ONE;
    private BitSet signedBS = DbcUtils.createSignedLongBitSet();
    private BitSet unsignedBS = DbcUtils.createUnsignedLongBitSet();
    private int bitAllocation = length;
    
    public DbcAttributeLong() {}
    
    @Override
    public void encode(final TypeData data, final byte[] bytes) {
        Long value = null;
        try {
            if ((value = data.toLong()) == null) {
                return;
            }
        } catch (EngineException e) {
            return;
        }
        
        final long scaled = Math.round((double) ((value.longValue() - offset)) / factor);
        if (order == DbcByteOrder.MOTOROLA) {
            encodeMotorola(scaled, bytes);
        } else {
            encodeIntel(scaled, bytes);
        }
    }
    
    public void encodeMotorola(final long scaled, final byte[] bytes) {
        if (lengthMinusOne == 0) {
            if (scaled != 0l) {
                bytes[startByte] |= (1 << startByteBit);
            }
            return;
        }
        
        int currentByte = startByte;
        int currentByteBit = startByteBit;
        BitSet bitset = DbcUtils.longToBs(scaled);
        
        for (int i = lengthMinusOne; i >= 0; --i) {
            if (currentByteBit < 0) {
                currentByteBit = 7;
                ++currentByte;
            }
            if (bitset.get(i)) {
                bytes[currentByte] |= (1 << currentByteBit);
            }
            --currentByteBit;
        }
    }
    
    public void encodeIntel(final long scaled, final byte[] bytes) {
        if (lengthMinusOne == 0) {
            if (scaled != 0l) {
                bytes[startByte] |= (1 << startByteBit);
            }
            return;
        }
        
        int currentByte = startByte;
        int currentByteBit = startByteBit;
        BitSet bitset = DbcUtils.longToBs(scaled);
        
        //start with least significant bit
        for (int i = 0; i < length; ++i) {
            if (currentByteBit > 7) {
                currentByteBit = 0;
                ++currentByte;
            }
            if (bitset.get(i)) {
                bytes[currentByte] |= (1 << currentByteBit);
            }
            ++currentByteBit;
        }
    }
    
    @Override
    public LongData decode(final byte[] bytes) {
        if (lengthMinusOne == 0) {
            return (bytes[startByte] & (1 << startByteBit)) != 0 ? oneOffsetData : zeroOffsetData;
        } else {
            return order == DbcByteOrder.MOTOROLA ? decodeMotorola(bytes) : decodeIntel(bytes);
        }
    }
    
    public LongData decodeMotorola(final byte[] bytes) {
        int currentByte = startByte;
        int currentByteBit = startByteBit;
        BitSet bitset = unsignedBS;
        
        //calculate with most significant bit
        if (signed && (bytes[currentByte] & (1 << currentByteBit)) != 0) {
            bitset = signedBS;
        }
        
        for (int i = lengthMinusOne; i >= 0; --i) {
            if (currentByteBit < 0) {
                currentByteBit = 7;
                ++currentByte;
            }
            bitset.set(i, (bytes[currentByte] & (1 << currentByteBit--)) != 0);
        }
        
        return LongData.nonNullValueOf(DbcUtils.bsToLong(bitset) * factor + offset);
    }
    
    public LongData decodeIntel(final byte[] bytes) {
        int currentByte = startByte;
        int currentByteBit = startByteBit;
        BitSet bitset = unsignedBS;
        
        //calculate with most significant bit
        if (signed && (bytes[endIntelByte] & (1 << endIntelByteBit)) != 0) {
            bitset = signedBS;
        }
        
        //start with least significant bit
        for (int i = 0; i < length; ++i) {
            if (currentByteBit > 7) {
                currentByteBit = 0;
                ++currentByte;
            }
            bitset.set(i, (bytes[currentByte] & (1 << currentByteBit++)) != 0);
        }
        
        return LongData.nonNullValueOf(DbcUtils.bsToLong(bitset) * factor + offset);
    }
    
    @Override
    public LongData interpret(final byte[] bytes) {
        return LongData.nonNullValueOf(DbcUtils.bsToLong(order == DbcByteOrder.MOTOROLA ? extractMotorola(bytes) : extractIntel(bytes)));
    }
    
    @Override
    public BitSet extract(final byte[] bytes) {
        return order == DbcByteOrder.MOTOROLA ? extractMotorola(bytes) : extractIntel(bytes);
    }
    
    public BitSet extractMotorola(final byte[] bytes) {
        int currentByte = startByte;
        int currentByteBit = startByteBit;
        BitSet bitset = unsignedBS;
        
        //calculate with most significant bit
        if (signed && (bytes[currentByte] & (1 << currentByteBit)) != 0) {
            bitset = signedBS;
        }
        
        for (int i = lengthMinusOne; i >= 0; --i) {
            if (currentByteBit < 0) {
                currentByteBit = 7;
                ++currentByte;
            }
            bitset.set(i, (bytes[currentByte] & (1 << currentByteBit--)) != 0);
        }
        return bitset;
    }
    
    public BitSet extractIntel(final byte[] bytes) {
        int currentByte = startByte;
        int currentByteBit = startByteBit;
        BitSet bitset = unsignedBS;
        
        //calculate with most significant bit
        if (signed && (bytes[endIntelByte] & (1 << endIntelByteBit)) != 0) {
            bitset = signedBS;
        }
        //start with least significant bit
        for (int i = 0; i < length; ++i) {
            if (currentByteBit > 7) {
                currentByteBit = 0;
                ++currentByte;
            }
            bitset.set(i, (bytes[currentByte] & (1 << currentByteBit++)) != 0);
        }

        return bitset;
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public int getHash() {
        return nameHash;
    }
    
    @Override
    public void setName(final String name) {
        this.name = name;
        this.nameHash = Hashing.getHash(name);
    }
    
    @Override
    public String getUnit() {
        return unit;
    }
    
    @Override
    public void setUnit(final String unit) {
        this.unit = unit;
    }
    
    @Override
    public boolean isWhole() {
        return true;
    }
    
    @Override
    public int getStartBit() {
        return startBit;
    }
    
    public void setStartBit(final int startBit) {
        this.startBit = startBit;
        this.startByte = startBit / 8;
        this.startByteBit = startBit % 8;
        this.endIntelByte = (startBit + length - 1) / 8;
        this.endIntelByteBit = (startBit + length - 1) % 8;
    }
    
    @Override
    public int getLength() {
        return length;
    }
    
    @Override
    public void setAlignedBitLength(final int allocation) {
        this.bitAllocation = allocation;
    }
    
    @Override
    public int getAlignedBitLength() {
        return this.bitAllocation;
    }
    
    @Override
    public DbcByteOrder getByteOrder() {
        return order;
    }
    
    public void setLength(final int length) {
        this.length = length;
        this.lengthMinusOne = length - 1;
        this.endIntelByte = (startBit + length - 1) / 8;
        this.endIntelByteBit = (startBit + length - 1) % 8;
        
        this.bitAllocation = (length > 8 && length % 8 != 0)
                ? (length / 8 + 1) * 8 //align bytes, some tools cannot support unaligned bytes
                : length;
    }
    
    public DbcByteOrder getOrder() {
        return order;
    }
    
    public void setOrder(final DbcByteOrder order) {
        this.order = order;
    }
    
    @Override
    public boolean isSigned() {
        return signed;
    }
    
    public void setSigned(final boolean signed) {
        this.signed = signed;
    }
    
    public long getInitialValue() {
        return offset;
    }
    
    @Override
    public BigDecimal getAdjustment() {
        return BigDecimal.valueOf(offset);
    }
    
    public void setAdjustment(final long offset) {
        this.offset = offset;
        this.zeroOffsetData = offset == 0l ? LongData.ZERO : LongData.valueOf(offset);
        this.oneOffsetData = LongData.valueOf(factor + offset);
    }
    
    @Override
    public BigDecimal getMultiplier() {
        return BigDecimal.valueOf(factor);
    }
    
    public void setMultiplier(final long factor) {
        this.factor = factor;
        this.oneOffsetData = LongData.valueOf(factor + offset);
    }
    
    public long getMinValue() {
        return minValue;
    }
    
    public void setMinValue(final long minValue) {
        this.minValue = minValue;
    }
    
    public long getMaxValue() {
        return maxValue;
    }
    
    public void setMaxValue(final long maxValue) {
        this.maxValue = maxValue;
    }
}
