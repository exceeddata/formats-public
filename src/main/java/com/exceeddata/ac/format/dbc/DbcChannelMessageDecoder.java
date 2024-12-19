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

public class DbcChannelMessageDecoder implements MessageDecoder {
    private static final long serialVersionUID = 1L;
    
    private LinkedHashMap<Integer, DbcChannel> channels;
    private boolean outputOffset;
    
    public DbcChannelMessageDecoder() {
        this(false);
    }
    
    public DbcChannelMessageDecoder(final boolean outputOffset) {
        this.channels = new LinkedHashMap<>();
        this.outputOffset = outputOffset;
    }
    
    private DbcChannelMessageDecoder(final DbcChannelMessageDecoder decoder) {
        this.channels = new LinkedHashMap<>(decoder.channels);
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
    public DbcChannelMessageDecoder clone() {
        return new DbcChannelMessageDecoder(this);
    }
    
    public DbcChannelMessageDecoder copy() {
        return new DbcChannelMessageDecoder(this);
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
        final DbcChannel channel = channels.get(message.getChannelID());
        return channel != null ? channel.decode(desc, message) : null;
    }
    
    @Override
    public Record decode(
            final MessageDesc desc, 
            final MessageContent message,
            final Record target) {
        final DbcChannel channel = channels.get(message.getChannelID());
        return channel != null ? channel.decode(desc, message, target) : null;
    }
    
    @Override
    public Record interpret(final MessageDesc desc, final MessageContent message) {
        final DbcChannel channel = channels.get(message.getChannelID());
        return channel != null ? channel.interpret(desc, message) : null;
    }
    
    @Override
    public Record interpret(
            final MessageDesc desc, 
            final MessageContent message,
            final Record target) {
        final DbcChannel channel = channels.get(message.getChannelID());
        return channel != null ? channel.interpret(desc, message, target) : null;
    }
    
    @Override
    public void select(final Set<String> selectedAttributes) {
        if (selectedAttributes == null || selectedAttributes.size() == 0) {
            return;
        }
        
        final ArrayList<Integer> unselectedChannels = new ArrayList<>();
        final ArrayList<Long> unselectedMessages = new ArrayList<>();
        final ArrayList<Integer> attributeIndices = new ArrayList<>();
        final Iterator<Map.Entry<Integer, DbcChannel>> citer = channels.entrySet().iterator();
        LinkedHashMap<Long, DbcMessage> messages = null;
        List<DbcAttribute> attributes = null;
        int attributeSizes = 0, indexSize = 0;
        Iterator<Map.Entry<Long, DbcMessage>> miter;
        Map.Entry<Integer, DbcChannel> centry;
        Map.Entry<Long, DbcMessage> mentry;
        
        for (int i = 0, s = channels.size(); i < s; ++i) {
            centry = citer.next();
            unselectedMessages.clear();
            messages = centry.getValue().getMessages();
            miter = messages.entrySet().iterator();
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
            
            //if no messages left, then clear the channel too
            if (messages.size() == 0) {
                unselectedChannels.add(centry.getKey());
            }
        }
        
        //clear all empty channels
        for (int i = 0, s = unselectedChannels.size(); i < s; ++i) {
            channels.remove(unselectedChannels.get(i));
        }
    }
    
    public LinkedHashMap<Integer, DbcChannel> getChannels() {
        return channels;
    }
    
    public void setChannels(final Map<Integer, DbcChannel> channels) {
        this.channels = new LinkedHashMap<>(channels);
    }
    
    public boolean containsChannel(final int channelID) {
        return channels.containsKey(channelID);
    }
    
    public DbcChannel getChannel(final int channelID) {
        return channels.get(channelID);
    }
    
    public void addChannel(final DbcChannel channel) {
        channels.put(channel.getChannelID(), channel);
    }
    
    public void removeChannel(final int channelID) {
        channels.remove(channelID);
    }
    
    public int size() {
        return channels.size();
    }
    
    public void clear() {
        channels.clear();
    }
}
