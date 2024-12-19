package com.exceeddata.ac.format.asc;

import com.exceeddata.ac.common.exception.EngineException;
import com.exceeddata.ac.format.asc.v11.Asc11Builder;
import com.exceeddata.ac.format.asc.v7.Asc7Builder;
import com.exceeddata.ac.format.asc.v8.Asc8Builder;

public final class AscBuilderFactory {
    
    private AscBuilderFactory() {}
    
    public static AscBuilder getBuilder(
            final String protocol,
            final String formatVersion) throws EngineException {
        if (formatVersion.startsWith("11.")) {
            return new Asc11Builder(protocol, formatVersion);
        } else if (formatVersion.startsWith("8.")) {
            return new Asc8Builder(protocol, formatVersion);
        } else if (formatVersion.startsWith("7.")) {
            return new Asc7Builder(protocol, formatVersion);
        } else {
            throw new EngineException("FORMAT_ASC_FORMAT_UNSUPPORTED: " + formatVersion);
        }
    }
}
