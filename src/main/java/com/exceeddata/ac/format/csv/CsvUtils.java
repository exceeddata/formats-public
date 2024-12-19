package com.exceeddata.ac.format.csv;

import java.util.HashSet;
import java.util.List;

import com.exceeddata.ac.common.util.SparseArray;
import com.exceeddata.ac.common.util.XNumberUtils;
import com.exceeddata.ac.common.util.XStringUtils;

public final class CsvUtils {
    private CsvUtils() {}
    
    public static String getHeaderSchema(final SparseArray<String> strings, final boolean validateCharacters) {
        final StringBuilder nsb = new StringBuilder(4096);
        final List<String> values = strings.datas();
        int vsize = strings.getSize();
        if (vsize != values.size()) {
            //validity check, sometimes header may contain extra comma at end
            while (vsize > values.size() && XStringUtils.isBlank(strings.get(vsize - 1))) {
                --vsize;
            }
            if (vsize != values.size()) {
                return null; //not equal, sparse row cannot be header
            }
        }
        
        final HashSet<String> metadatas = new HashSet<String>(vsize);
        String name;
        boolean validHeader = true;
        
        if (validateCharacters) {
            for (int i = 0; validHeader && i < vsize; ++i) {
                name = values.get(i);
                if (XStringUtils.isBlank(name) || XNumberUtils.isNumber(name)) {
                    validHeader = false;
                } else {
                    switch (name.toLowerCase()) {
                        case "null":
                        case "-":
                        case "--":
                        case "+":
                        case "/":
                        case "*":
                        case "%":
                        case "&":
                        case "&&":
                        case "and":
                        case "or":
                        case "|":
                        case "||":
                        case "=":
                        case "==":
                        case "===": validHeader = false; break;
                        default:
                            metadatas.add(name);
                            nsb.append("'").append(name).append("' string, ");
                    }
                }
            }
        } else {
            for (int i = 0; validHeader && i < vsize; ++i) {
                name = values.get(i);
                if (XStringUtils.isBlank(name)) {
                    String name2 = "field" + i;
                    if (metadatas.contains(name2)) {
                        for (int j = 2; ; ++j) {
                            name2 = "field" + i + "_" + j;
                            if (!metadatas.contains(name2)) {
                                metadatas.add(name2);
                                nsb.append("'").append(name2).append("' string, ");
                                break;
                            }
                        }
                    } else {
                        metadatas.add(name2);
                        nsb.append("'").append(name2).append("' string, ");
                    }
                } else if (metadatas.contains(name)) {
                    for (int j = 2; ; ++j) {
                        String name2 = name + "_" + j;
                        if (!metadatas.contains(name2)) {
                            metadatas.add(name2);
                            nsb.append("'").append(name2).append("' string, ");
                            break;
                        }
                    }
                } else {
                    metadatas.add(name);
                    nsb.append("'").append(name).append("' string, ");
                }
            }
        }
        
        //validate, repeat column names are more likely to be data
        validHeader = validHeader && (metadatas.size() == vsize);
        
        if (validHeader) {
            nsb.setLength(nsb.length() - 2);
            return nsb.toString();
        } else {
            return null;
        }
    }
}
