package com.exceeddata.ac.format.dbc;

/**
 * A null delegator class that does nothing.
 *
 */
public class DbcDelegatorNull implements DbcDelegator {
    private static final long serialVersionUID = 1L;
    
    public static final DbcDelegatorNull INSTANCE = new DbcDelegatorNull();
    
    private DbcDelegatorNull() {
    }

    @Override
    public void delegate(final DbcChannel channel, final String line, final boolean useQualifiedName, final boolean applyFormula) {
    }
}
