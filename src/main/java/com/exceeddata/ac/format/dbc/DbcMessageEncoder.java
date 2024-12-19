package com.exceeddata.ac.format.dbc;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import com.exceeddata.ac.common.data.record.Record;
import com.exceeddata.ac.common.message.MessageContent;
import com.exceeddata.ac.common.message.MessageEncoder;
import com.exceeddata.ac.common.message.MessagePacket;

public class DbcMessageEncoder implements MessageEncoder {
    private static final long serialVersionUID = 1L;
    
    private LinkedHashMap<String, DbcMessage> attributeMessages = null;
    private LinkedHashSet<String> attributeNames = null;
    
    public DbcMessageEncoder() {
        this.attributeMessages = new LinkedHashMap<>();
        this.attributeNames = new LinkedHashSet<>();
    }
    
    public DbcMessageEncoder(final DbcMessageEncoder encoder) {
        if (encoder != null) {
            this.attributeMessages = new LinkedHashMap<>(encoder.attributeMessages);
        } else {
            this.attributeMessages = new LinkedHashMap<>();
        }
        this.attributeNames = new LinkedHashSet<>();
    }
    
    @Override
    public DbcMessageEncoder clone() {
        return new DbcMessageEncoder(this);
    }
    
    public DbcMessageEncoder copy() {
        return new DbcMessageEncoder(this);
    }
    
    @Override
    public ArrayList<MessageContent> encode(final Record record) {
        for (int i = 0, s = record.size(); i < s; ++i) {
            if (!record.dataAt(i).isEmpty()) {
                attributeNames.add(record.nameAt(i));
            }
        }
        
        final ArrayList<MessageContent> contents = new ArrayList<>(attributeNames.size() / 8 + 5);
        DbcMessage message;
        String name;
        byte[] data;
        
        while (attributeNames.size() > 0) {
            name = attributeNames.iterator().next();
            if ((message = attributeMessages.get(name)) != null) {
                if ((data = message.encode(record)) != null) {
                    final MessagePacket packet = new MessagePacket();
                    packet.setData(data);
                    packet.setDataLength(data.length);
                    packet.setChannelID(message.getChannelID());
                    packet.setMessageID(message.getMessageID());
                    contents.add(packet);
                }
                for (final DbcAttribute attribute : message.getAttributes()) {
                    attributeNames.remove(attribute.getName());
                }
            } else {
                attributeNames.remove(name);
            }
        }
        
        return contents;
    }
    
    public LinkedHashMap<String, DbcMessage> getAttributeMessages() {
        return attributeMessages;
    }
    
    public DbcMessageEncoder setAttributeMessages(final Map<String, DbcMessage> attributeMessages) {
        this.attributeMessages = new LinkedHashMap<>(attributeMessages);
        return this;
    }
    
    public boolean containsAttribute(final String attributeName) {
        return attributeMessages.containsKey(attributeName);
    }
    
    public DbcMessage getAttribute(final String attributeName) {
        return attributeMessages.get(attributeName);
    }
    
    public void addMessage(final DbcMessage message) {
        for (final DbcAttribute attribute : message.getAttributes()) {
            attributeMessages.put(attribute.getName(), message);
        }
    }
    
    public void removeMessage(final String attributeName) {
        attributeMessages.remove(attributeName);
    }
    
    public int size() {
        return attributeMessages.size();
    }
    
    public void clear() {
        attributeMessages.clear();
    }
}
