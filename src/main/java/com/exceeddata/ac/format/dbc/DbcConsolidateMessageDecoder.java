package com.exceeddata.ac.format.dbc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.exceeddata.ac.common.data.record.Record;
import com.exceeddata.ac.common.message.MessageContent;
import com.exceeddata.ac.common.message.MessageDecoder;
import com.exceeddata.ac.common.message.MessageDesc;

public class DbcConsolidateMessageDecoder implements MessageDecoder {
    private static final long serialVersionUID = 1L;
    
    private LinkedHashMap<Long, DbcMessage> messages;
    private boolean outputOffset;
    
    public DbcConsolidateMessageDecoder() {
        this(false);
    }
    
    public DbcConsolidateMessageDecoder(final boolean outputOffset) {
        this.messages = new LinkedHashMap<>();
        this.outputOffset = outputOffset;
    }
    
    private DbcConsolidateMessageDecoder(final DbcConsolidateMessageDecoder decoder) {
        this.messages = new LinkedHashMap<>(decoder.messages);
        this.outputOffset = decoder.outputOffset;
    }
    
    @Override
    public boolean getOutputOffset() {
        return outputOffset;
    }
    
    public void setOutputOffset(final boolean outputOffset) {
        this.outputOffset = outputOffset;
    }
    
    @Override
    public DbcConsolidateMessageDecoder clone() {
        return new DbcConsolidateMessageDecoder(this);
    }
    
    public DbcConsolidateMessageDecoder copy() {
        return new DbcConsolidateMessageDecoder(this);
    }
    
    @Override
    public Record compute(final MessageDesc desc, final MessageContent message, final boolean applyFormula) {
        return applyFormula ? decode(desc, message) : interpret(desc, message);
    }
    
    @Override
    public Record compute(
            final MessageDesc desc, 
            final MessageContent message,
            final Record target,
            final boolean applyFormula) {
        return applyFormula ? decode(desc, message, target) : interpret(desc, message, target);
    }
    
    @Override
    public Record decode(final MessageDesc desc, final MessageContent message) {
        final DbcMessage dbc = messages.get(message.getMessageID());
        return dbc != null ? dbc.decode(desc, message) : null;
    }
    
    @Override
    public Record decode(
            final MessageDesc desc, 
            final MessageContent message,
            final Record target) {
        final DbcMessage dbc = messages.get(message.getMessageID());
        return dbc != null ? dbc.decode(desc, message, target) : null;
    }
    
    @Override
    public Record interpret(final MessageDesc desc, final MessageContent message) {
        final DbcMessage dbc = messages.get(message.getMessageID());
        return dbc != null ? dbc.interpret(desc, message) : null;
    }
    
    @Override
    public Record interpret(
            final MessageDesc desc, 
            final MessageContent message,
            final Record target) {
        final DbcMessage dbc = messages.get(message.getMessageID());
        return dbc != null ? dbc.interpret(desc, message, target) : null;
    }
    
    @Override
    public void select(final Set<String> selectedAttributes) {
        if (selectedAttributes == null || selectedAttributes.size() == 0) {
            return;
        }
        
        final ArrayList<Long> unselectedMessages = new ArrayList<>();
        final ArrayList<Integer> attributeIndices = new ArrayList<>();
        final Iterator<Map.Entry<Long, DbcMessage>> miter = messages.entrySet().iterator();
        List<DbcAttribute> attributes = null;
        int attributeSizes = 0, indexSize = 0;
        Map.Entry<Long, DbcMessage> mentry;
        
        for (int j = 0, t = messages.size(); j < t; ++j) {
            mentry = miter.next();
            attributeIndices.clear();
            attributes = mentry.getValue().getAttributes();
            attributeSizes = attributes.size();
            
            for (int k = 0; k < attributeSizes; ++k) {
                if (selectedAttributes.contains(attributes.get(k).getName())) {
                    attributeIndices.add(k);
                }
            }
            
            indexSize = attributeIndices.size();
            if (indexSize == 0) {
                //no match, clear the entire message
                unselectedMessages.add(mentry.getKey());
            } else if (indexSize != attributeSizes) {
                //some match, reconstruct the attributes
                final List<DbcAttribute> newAttributes = new ArrayList<DbcAttribute>();
                for (int l = 0, v = attributeIndices.size(); l < v; ++l) {
                    newAttributes.add(attributes.get(attributeIndices.get(l)));
                }
                mentry.getValue().setAttributes(newAttributes);
            }
        }
        
        //clear all empty messages
        for (int j = 0, t = unselectedMessages.size(); j < t; ++j) {
            messages.remove(unselectedMessages.get(j));
        }
    }
    
    public void addChannel(final DbcChannel channel) {
        for (final DbcMessage message : channel.getMessages().values()) {
            if (!messages.containsKey(message.getMessageID())) { //respect the dbc order in case of duplicate id
                messages.put(message.getMessageID(), message);
            }
        }
    }
    
    public void addMessage(final DbcMessage message) {
        if (!messages.containsKey(message.getMessageID())) { //respect the dbc order in case of duplicate id
            messages.put(message.getMessageID(), message);
        }
    }
    
    public LinkedHashMap<Long, DbcMessage> getMessages() {
        return messages;
    }
    
    public int size() {
        return messages.size();
    }
    
    public void clear() {
        messages.clear();
    }
}
