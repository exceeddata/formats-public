package com.exceeddata.ac.format.dbc;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.BitSet;

import com.exceeddata.ac.common.data.typedata.TypeData;

public interface DbcAttribute extends Serializable {
    
    /**
     * Get the name of the attribute.
     * 
     * @return name
     */
    public String getName();
    
    /**
     * Get the default hash of the name.
     * 
     * @return int
     */
    public int getHash();
    
    /**
     * Set the name of the attribute.
     * 
     * @param name the name
     */
    public void setName(String name);

    /**
     * Get the unit of the attribute, if applicable, otherwise null.
     * 
     * @return unit
     */
    public String getUnit();
    
    /**
     * Set the unit of the attribute, if applicable.
     * 
     * @param name the name
     */
    public void setUnit(String unit);
    
    /**
     * Encode data in-place into bytes.
     * 
     * @param data the value
     * @param bytes the to-write bytes
     */
    public void encode(TypeData data, byte[] bytes);
    
    /**
     * Decode bytes into formula-computed data.
     * 
     * @param bytes the bytes
     * @return TypeData
     */
    public TypeData decode(byte[] bytes);
    
    /**
     * Interpret bytes into raw-uncomputed value.
     * 
     * @param bytes the bytes
     * @return TypeData
     */
    public TypeData interpret(byte[] bytes);
    
    /**
     * Extract bytes into Big Endian raw bits.
     * 
     * @param bytes the bytes
     * @return BitSet
     */
    public BitSet extract(byte[] bytes);
    
    /**
     * Return whether the attribute is whole number.
     * 
     * @return true or false
     */
    public boolean isWhole();
    
    /**
     * Return whether the attribute is signed.
     * 
     * @return true or false
     */
    public boolean isSigned();
    
    /**
     * Return the start bit of the attribute.
     * 
     * @return int
     */
    public int getStartBit();

    
    /**
     * Set the length of the attribute, if applicable.
     * 
     * @param name the name
     */
    public void setLength(int length);
    
    /**
     * Return the aligned bit length (may be bigger than actual length). Some tools can only support aligned length if it is bigger than 1 byte.
     * 
     * @return int
     */
    public int getAlignedBitLength();

    
    /**
     * Set the aligned bit length to allocate. Some tools can only support aligned length if it is bigger than 1 byte.
     * 
     * @param name the name
     */
    public void setAlignedBitLength(int length);
    
    /**
     * Return the bit length of the attribute.
     * 
     * @return int
     */
    public int getLength();
    
    /**
     * Return the multiplier factor of the attribute.
     * 
     * @return BigDecimal
     */
    public BigDecimal getMultiplier();
    
    /**
     * Return the adjustment offset of the attribute.
     * 
     * @return BigDecimal
     */
    public BigDecimal getAdjustment();
    
    /**
     * Return the encoding byte order.
     * 
     * @return DbcOrder
     */
    public DbcByteOrder getByteOrder();
}
