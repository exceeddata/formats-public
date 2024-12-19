package com.exceeddata.ac.format.dbc;

import java.math.BigDecimal;

public class DbcDelegatorMessage implements DbcDelegator {
    private static final long serialVersionUID = 1L;
    
    private DbcMessage message = null;
    
    public DbcDelegatorMessage(final int channelID, final String line) {
        if (!line.startsWith("BO_ ")) {
            throw new RuntimeException("FORMAT_DBC_MESSAGE_UNEXPECTED: " + line);
        }
        
        final String bo_id, bo_name, bo_bytes;
        int index = 4;
        String remains = line.substring(index).trim();
        
        if ((index = remains.indexOf(' ')) <= 0) {
            throw new RuntimeException("FORMAT_DBC_MESSAGE_UNEXPECTED: " + line);
        }
        bo_id = remains.substring(0, index).trim();
        remains = remains.substring(index + 1);
        
        if ((index = remains.indexOf(':')) <= 0) {
            throw new RuntimeException("FORMAT_DBC_MESSAGE_UNEXPECTED: " + line);
        }
        bo_name = remains.substring(0, index).trim();
        remains = remains.substring(index + 1).trim();
        
        if ((index = remains.indexOf(' ')) <= 0) {
            throw new RuntimeException("FORMAT_DBC_MESSAGE_UNEXPECTED: " + line);
        }
        bo_bytes = remains.substring(0, index).trim();
        
        final String messageName = bo_name;
        final long messageID = Long.parseLong(bo_id);
        final int length = Integer.parseInt(bo_bytes);
        
        message = new DbcMessage(channelID, messageID, messageName, length);
    }

    /**
     * Match line with BO_ message.  The line must be trimmed before parsing in.
     * 
     * @param line the DBC line
     * @return true or false
     */
    public static boolean matchesMessage(final String line) {
        if (!line.startsWith("BO_ ")) {
            return false;
        }
        
        final String bo_id, bo_bytes;
        int index = 4;
        String remains = line.substring(index).trim();
        
        if ((index = remains.indexOf(' ')) <= 0) {
            return false;
        }
        bo_id = remains.substring(0, index).trim();
        remains = remains.substring(index + 1);
        
        if ((index = remains.indexOf(':')) <= 0) {
            return false;
        }
        remains = remains.substring(index + 1).trim();
        
        if ((index = remains.indexOf(' ')) <= 0) {
            return false;
        }
        bo_bytes = remains.substring(0, index).trim();
        
        try {
            Long.parseLong(bo_id);
            Integer.parseInt(bo_bytes);
            return true;
        } catch (NumberFormatException e) {
        }
        return false;
    }

    /**
     * Match line with SG_ attribute.  The line must be trimmed before parsing in.
     * 
     * @param line the DBC line
     * @return true or false
     */
    public static boolean matchesAttribute(String line) {
        if (!line.startsWith("SG_ ")) {
            return false;
        }
        
        final String sg_start, sg_len, sg_endian, sg_factor, sg_offset, sg_min, sg_max;
        int index = 4;
        String remains = line.substring(index);
        
        if ((index = remains.indexOf(':')) <= 0) {
            return false;
        }
        remains = remains.substring(index + 1);
        
        if ((index = remains.indexOf('|')) <= 0) {
            return false;
        }
        sg_start = remains.substring(0, index).trim();
        remains = remains.substring(index + 1);
        
        if ((index = remains.indexOf('@')) <= 0) {
            return false;
        }
        sg_len = remains.substring(0, index).trim();
        remains = remains.substring(index + 1);
        
        if ((index = remains.indexOf('(')) <= 0) {
            return false;
        }
        sg_endian = remains.substring(0, index).trim();
        remains = remains.substring(index + 1);
        
        if ((index = remains.indexOf(',')) <= 0) {
            return false;
        }
        sg_factor = remains.substring(0, index).trim();
        remains = remains.substring(index + 1);
        
        if ((index = remains.indexOf(')')) <= 0) {
            return false;
        }
        sg_offset = remains.substring(0, index).trim();
        remains = remains.substring(index + 1);

        if ((index = remains.indexOf('[')) <= 0) {
            return false;
        }
        remains = remains.substring(index + 1);
        
        if ((index = remains.indexOf('|')) <= 0) {
            return false;
        }
        sg_min = remains.substring(0, index).trim();
        remains = remains.substring(index + 1);
        
        if ((index = remains.indexOf(']')) <= 0) {
            return false;
        }
        sg_max = remains.substring(0, index).trim();
        
        try {
            Integer.parseInt(sg_start);
            Integer.parseInt(sg_len);
            
            if (sg_endian.length() != 2 || (sg_endian.charAt(0) != '0' && sg_endian.charAt(0) != '1') || (sg_endian.charAt(1) != '+' && sg_endian.charAt(1) != '-') ) {
                return false;
            }
            
            Double.parseDouble(sg_factor);
            Double.parseDouble(sg_offset);
            Double.parseDouble(sg_min);
            Double.parseDouble(sg_max);
            
            return true;
        } catch (NumberFormatException e) {
        }
        
        return false;
    }
    
    @Override
    public void delegate(
            final DbcChannel channel, 
            final String line, 
            final boolean useQualifiedName,
            final boolean applyFormula) {
        if (!channel.containsMessage(message.getMessageID())) {
            channel.addMessage(message);
        }
        
        final DbcAttribute attribute = buildAttribute(line, message.getName(), message.getLength(), useQualifiedName, applyFormula);
        if (attribute != null) {
            message.addAttribute(attribute);
        }
    }
    
    public static DbcAttribute buildAttribute(
            final String line,
            final String messageName, 
            final int messageLength,
            final boolean useQualifiedName,
            final boolean applyFormula) {
        final String sg_name, sg_start, sg_len, sg_endian, sg_factor, sg_offset, sg_min, sg_max;
        int index = 4;
        String remains = line.substring(index);
        
        if ((index = remains.indexOf(':')) <= 0) {
            throw new RuntimeException("FORMAT_DBC_MESSAGE_UNEXPECTED: " + line);
        }
        sg_name = remains.substring(0, index).trim();
        remains = remains.substring(index + 1);
        
        if ((index = remains.indexOf('|')) <= 0) {
            throw new RuntimeException("FORMAT_DBC_MESSAGE_UNEXPECTED: " + line);
        }
        sg_start = remains.substring(0, index).trim();
        remains = remains.substring(index + 1);
        
        if ((index = remains.indexOf('@')) <= 0) {
            throw new RuntimeException("FORMAT_DBC_MESSAGE_UNEXPECTED: " + line);
        }
        sg_len = remains.substring(0, index).trim();
        remains = remains.substring(index + 1);
        
        if ((index = remains.indexOf('(')) <= 0) {
            throw new RuntimeException("FORMAT_DBC_MESSAGE_UNEXPECTED: " + line);
        }
        sg_endian = remains.substring(0, index).trim();
        remains = remains.substring(index + 1);
        
        if (sg_endian.length() != 2 || (index = remains.indexOf(',')) <= 0) {
            throw new RuntimeException("FORMAT_DBC_MESSAGE_UNEXPECTED: " + line);
        }
        sg_factor = remains.substring(0, index).trim();
        remains = remains.substring(index + 1);
        
        if ((index = remains.indexOf(')')) <= 0) {
            throw new RuntimeException("FORMAT_DBC_MESSAGE_UNEXPECTED: " + line);
        }
        sg_offset = remains.substring(0, index).trim();
        remains = remains.substring(index + 1);

        if ((index = remains.indexOf('[')) <= 0) {
            throw new RuntimeException("FORMAT_DBC_MESSAGE_UNEXPECTED: " + line);
        }
        remains = remains.substring(index + 1);
        
        if ((index = remains.indexOf('|')) <= 0) {
            throw new RuntimeException("FORMAT_DBC_MESSAGE_UNEXPECTED: " + line);
        }
        sg_min = remains.substring(0, index).trim();
        remains = remains.substring(index + 1);
        
        if ((index = remains.indexOf(']')) <= 0) {
            throw new RuntimeException("FORMAT_DBC_MESSAGE_UNEXPECTED: " + line);
        }
        sg_max = remains.substring(0, index).trim();
        remains = remains.substring(index + 1).trim();
        
        final String name = useQualifiedName ? messageName + "." + sg_name : sg_name;
        final int startBit = Integer.parseInt(sg_start);
        final int bitLength = Integer.parseInt(sg_len);
        final DbcByteOrder order = '0' == sg_endian.charAt(0) ? DbcByteOrder.MOTOROLA : DbcByteOrder.INTEL;
        final boolean signed = '-' == sg_endian.charAt(1);
        final BigDecimal factor = !applyFormula || "1".equals(sg_factor) ? BigDecimal.ONE : new BigDecimal(sg_factor); //if not formula set factor to 1
        final BigDecimal offset = !applyFormula || "0".equals(sg_offset) ? BigDecimal.ZERO : new BigDecimal(sg_offset); //if not formula set offset to 0
        final BigDecimal minValue = "0".equals(sg_min) ? BigDecimal.ZERO : new BigDecimal(sg_min);
        final BigDecimal maxValue = "1".equals(sg_max) ? BigDecimal.ONE : new BigDecimal(sg_max);
        final boolean whole = factor.scale() <= 0 && offset.scale() <= 0 && minValue.scale() <=0 && maxValue.scale() <= 0;
        final int maxIntBits = signed ? 32 : 31;
        final int maxLongBits = signed ? 64 : 63;
        
        String unit = null;
        if (remains.length() != 0) {
            if ((index = remains.indexOf('"')) >= 0) {
                remains = remains.substring(index + 1);
                if ((index = remains.indexOf('"')) >= 0) {
                    unit = remains.substring(0, index).trim();
                }
            }
        }
        
        if (whole) {
            if (bitLength <= maxLongBits) {
                if (bitLength > maxIntBits) {
                    final DbcAttributeLong attribute = new DbcAttributeLong();
                    attribute.setName(name);
                    attribute.setUnit(unit);
                    attribute.setStartBit(startBit);
                    attribute.setLength(bitLength);
                    attribute.setOrder(order);
                    attribute.setSigned(signed);
                    attribute.setMultiplier(factor.longValue());
                    attribute.setAdjustment(offset.longValue());
                    attribute.setMinValue(minValue.longValue());
                    attribute.setMaxValue(maxValue.longValue());
                    return attribute;
                } else {
                    final DbcAttributeInteger attribute = new DbcAttributeInteger();
                    attribute.setName(name);
                    attribute.setUnit(unit);
                    attribute.setStartBit(startBit);
                    attribute.setLength(bitLength);
                    attribute.setOrder(order);
                    attribute.setSigned(signed);
                    attribute.setMultiplier(factor.intValue());
                    attribute.setAdjustment(offset.intValue());
                    attribute.setMinValue(minValue.intValue());
                    attribute.setMaxValue(maxValue.intValue());
                    return attribute;
                }
            } else {
                final DbcAttributeDecimal attribute = new DbcAttributeDecimal();
                attribute.setName(name);
                attribute.setUnit(unit);
                attribute.setStartBit(startBit);
                attribute.setLength(bitLength);
                attribute.setOrder(order);
                attribute.setSigned(signed);
                attribute.setMultiplier(factor);
                attribute.setAdjustment(offset);
                attribute.setMinValue(minValue);
                attribute.setMaxValue(maxValue);
                return attribute;
            }
        }
        
        final DbcAttributeDouble attribute = new DbcAttributeDouble();
        attribute.setName(name);
        attribute.setUnit(unit);
        attribute.setStartBit(startBit);
        attribute.setLength(bitLength);
        attribute.setOrder(order);
        attribute.setSigned(signed);
        attribute.setMultiplier(factor);
        attribute.setAdjustment(offset);
        attribute.setMinValue(minValue);
        attribute.setMaxValue(maxValue);
        return attribute;
    }
}
