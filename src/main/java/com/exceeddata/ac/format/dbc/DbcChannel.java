package com.exceeddata.ac.format.dbc;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import com.exceeddata.ac.common.data.record.Record;
import com.exceeddata.ac.common.message.MessageContent;
import com.exceeddata.ac.common.message.MessageDesc;

public class DbcChannel implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private int channelID = 1;
    private LinkedHashMap<Long, DbcMessage> messages = null;
    
    public DbcChannel() {
        this.messages = new LinkedHashMap<>();
    }
    
    private DbcChannel(final DbcChannel channel) {
        this.channelID = channel.channelID;
        this.messages = new LinkedHashMap<>(channel.messages);
    }
    
    @Override
    public DbcChannel clone() {
        return new DbcChannel(this);
    }
    
    public DbcChannel copy() {
        return new DbcChannel(this);
    }
    
    public Record decode(final MessageDesc desc, final MessageContent content) {
        final DbcMessage dbc = messages.get(content.getMessageID());
        return dbc != null ? dbc.decode(desc, content) : null;
    }
    
    public Record decode(
            final MessageDesc desc, 
            final MessageContent content,
            final Record target) {
        final DbcMessage dbc = messages.get(content.getMessageID());
        return dbc != null ? dbc.decode(desc, content, target) : null;
    }
    
    public Record interpret(final MessageDesc desc, final MessageContent content) {
        final DbcMessage dbc = messages.get(content.getMessageID());
        return dbc != null ? dbc.interpret(desc, content) : null;
    }
    
    public Record interpret(
            final MessageDesc desc, 
            final MessageContent content,
            final Record target) {
        final DbcMessage dbc = messages.get(content.getMessageID());
        return dbc != null ? dbc.interpret(desc, content, target) : null;
    }
    
    public int getChannelID() {
        return channelID;
    }
    
    public void setChannelID(final int channelID) {
        this.channelID = channelID;
    }
    
    public LinkedHashMap<Long, DbcMessage> getMessages() {
        return messages;
    }
    
    public void setMessages(final Map<Long, DbcMessage> messages) {
        this.messages = new LinkedHashMap<>(messages);
    }
    
    public boolean containsMessage(final long msgid) {
        return messages.containsKey(msgid);
    }
    
    public DbcMessage getMessage(final long msgid) {
        return messages.get(msgid);
    }
    
    public void addMessage(final DbcMessage message) {
        messages.put(message.getMessageID(), message);
    }
    
    public void removeMessage(final long msgid) {
        messages.remove(msgid);
    }
    
    public int size() {
        return messages.size();
    }
    
    public void clear() {
        messages.clear();
    }
}
