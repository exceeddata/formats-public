package com.exceeddata.ac.format.dbc;

import java.io.Serializable;

/**
 * An interface for DBC Delegator.
 *
 */
public interface DbcDelegator extends Serializable {
    
    /**
     * Delegate the line parsing to delegator.
     * 
     * @param channel the channel
     * @param line the line
     * @param useQualifiedName whether to construct a qualified name
     * @param applyFormula whether to apply formula
     */
    public void delegate(DbcChannel channel, String line, boolean useQualifiedName, boolean applyFormula);
}
