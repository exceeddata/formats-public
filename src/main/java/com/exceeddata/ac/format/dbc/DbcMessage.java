package com.exceeddata.ac.format.dbc;

import static com.exceeddata.ac.common.message.MessageConstants.OFFSET;
import static com.exceeddata.ac.common.message.MessageConstants.OFFSET_HASH;
import static com.exceeddata.ac.common.message.MessageConstants.TIME;
import static com.exceeddata.ac.common.message.MessageConstants.TIME_HASH;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.exceeddata.ac.common.data.record.Record;
import com.exceeddata.ac.common.data.typedata.InstantData;
import com.exceeddata.ac.common.data.typedata.LongData;
import com.exceeddata.ac.common.data.typedata.NullData;
import com.exceeddata.ac.common.message.MessageContent;
import com.exceeddata.ac.common.message.MessageDesc;

public class DbcMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private int channelID = 0;
    private long messageID = 0l; //need to take off the extended frame high-bit
    private String name = null;
    private int length = 0;
    private boolean extendedFrame = false;
    private boolean outputOffset = false;
    
    private List<DbcAttribute> attributes = null;
    private Record template = null;
    
    public DbcMessage() {
        this.attributes = new ArrayList<DbcAttribute>();
    }
    
    public DbcMessage(
            final int channelID, 
            final long messageID, 
            final String name, 
            final int length) {
        this.channelID = channelID;
        this.messageID = messageID & 0x7fffffff; //take off the extended frame high-bit
        this.name = name;
        this.length = length;
        this.attributes = new ArrayList<DbcAttribute>();
        this.extendedFrame = (messageID & 0x80000000) != 0l;
    }
    
    private DbcMessage(final DbcMessage message) {
        this.channelID = message.channelID;
        this.messageID = message.messageID;
        this.name = message.name;
        this.length = message.length;
        this.outputOffset = message.outputOffset;
        this.attributes = new ArrayList<DbcAttribute>(message.attributes);
        this.extendedFrame = message.extendedFrame;
    }
    
    public void setOutputOffset(final boolean outputOffset) {
        this.outputOffset = outputOffset;
    }
    
    @Override
    public DbcMessage clone() {
        return new DbcMessage(this);
    }
    
    public DbcMessage copy() {
        return new DbcMessage(this);
    }
    
    public byte[] encode(final Record record) {
        final byte[] bytes = new byte[length];
        for (final DbcAttribute attribute : attributes) {
            attribute.encode(record.get(attribute.getName(), attribute.getHash()), bytes);
        }
        return bytes;
    }
    
    public Record decode(final MessageDesc desc, final MessageContent content) {
        if (template == null) {
            template = new Record();
            template.add(TIME, TIME_HASH, NullData.INSTANCE);
            if (outputOffset) {
                template.add(OFFSET, OFFSET_HASH, NullData.INSTANCE);
            }
            for (int i = 0, size = attributes.size(); i < size; ++i) {
                template.add(attributes.get(i).getName(), attributes.get(i).getHash(), NullData.INSTANCE);
            }
        }
        return decode(desc, content, template.dataCopy());
    }
    
    public Record decode(
            final MessageDesc desc, 
            final MessageContent content,
            final Record target) {
        final byte[] bytes = content.getData();
        if (bytes.length < length) { //check invalid bytes, sometimes the dbc may be mismatched with bytes
            return null;
        }
        
        final Instant start = desc.getTimeStart();
        final long nanosOffset = content.getNanosOffset();
        DbcAttribute attribute;
        
        target.setAt(0, start != null ? new InstantData(start.plusNanos(nanosOffset)) : InstantData.NULL);
        if (outputOffset) {
            target.setAt(1, new LongData(nanosOffset));
        }
        
        for (int i = 0, s = attributes.size(); i < s; ++i) {
            attribute = attributes.get(i);
            target.add(attribute.getName(), attribute.getHash(), attribute.decode(bytes));
        }
        return target;
    }
    
    public Record interpret(final MessageDesc desc, final MessageContent content) {
        if (template == null) {
            template = new Record();
            template.add(TIME, TIME_HASH, NullData.INSTANCE);
            if (outputOffset) {
                template.add(OFFSET, OFFSET_HASH, NullData.INSTANCE);
            }
            for (int i = 0, size = attributes.size(); i < size; ++i) {
                template.add(attributes.get(i).getName(), attributes.get(i).getHash(), NullData.INSTANCE);
            }
        }
        return interpret(desc, content, template.dataCopy());
    }
    
    public Record interpret(
            final MessageDesc desc, 
            final MessageContent content,
            final Record target) {
        final byte[] bytes = content.getData();
        if (bytes.length < length) { //check invalid bytes, sometimes the dbc may be mismatched with bytes
            return null;
        }
        
        final Instant start = desc.getTimeStart();
        final long nanosOffset = content.getNanosOffset();
        DbcAttribute attribute;
        
        target.setAt(0, start != null ? new InstantData(start.plusNanos(nanosOffset)) : InstantData.NULL);
        if (outputOffset) {
          target.setAt(1, new LongData(nanosOffset));
        }
        
        for (int i = 0, s = attributes.size(); i < s; ++i) {
            attribute = attributes.get(i);
            target.add(attribute.getName(), attribute.getHash(), attribute.interpret(bytes));
        }
        return target;
    }
    
    public Record inspect(final MessageDesc desc, final MessageContent content, final Set<String> selectedAttributes) {
        return inspect(desc, content, selectedAttributes, new Record());
    }
    
    public Record inspect(final MessageDesc desc, final MessageContent content, final Set<String> selectedAttributes, final Record target) {
        final byte[] bytes = content.getData();
        if (bytes.length < length) { //check invalid bytes, sometimes the dbc may be mismatched with bytes
            return null;
        }
        
        final Instant start = desc.getTimeStart();
        final long nanosOffset = content.getNanosOffset();
        DbcAttribute attribute;
        
        target.setAt(0, start != null ? new InstantData(start.plusNanos(nanosOffset)) : InstantData.NULL);
        if (outputOffset) {
            target.setAt(1, new LongData(nanosOffset));
        }

        for (int i = 0, s = attributes.size(); i < s; ++i) {
            attribute = attributes.get(i);
            if (selectedAttributes.contains(attribute.getName())) {
                target.add(attribute.getName(), attribute.getHash(), attribute.decode(bytes));
            }
        }
        return target.size() == 2 ? null : target;    //not found
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
    
    public void setMessageID(final long messageID) {
        this.messageID = messageID & 0x7fffffff;
        this.extendedFrame = (messageID & 0x80000000) != 0l;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(final String name) {
        this.name = name;
    }
    
    public int getLength() {
        return length;
    }
    
    public void setLength(final int length) {
        this.length = length;
    }
    
    public List<DbcAttribute> getAttributes() {
        return attributes;
    }
    
    public void setAttributes(final List<DbcAttribute> attributes) {
        this.attributes = attributes;
        this.template = null;
    }
    
    public void addAttribute(final DbcAttribute attribute) {
        this.attributes.add(attribute);
        this.template = null;
    }
    
    public void setAttribute(final int index, final DbcAttribute attribute) {
        this.attributes.set(index, attribute);
        this.template = null;
    }
    
    public void removeAttribute(final int index) {
        this.attributes.remove(index);
        this.template = null;
    }
    
    public int size() {
        return attributes.size();
    }
    
    public void clear() {
        this.attributes.clear();
        this.template = null;
    }
    
    public boolean isExtendedFrame() {
        return extendedFrame;
    }
    
    public void setExtendedFrame(final boolean extendedFrame) {
        this.extendedFrame = extendedFrame;
    }
}
