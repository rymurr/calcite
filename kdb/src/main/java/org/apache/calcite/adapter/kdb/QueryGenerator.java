package org.apache.calcite.adapter.kdb;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class QueryGenerator {
    public static String generate(String collectionName, String filterJson, String projectJson, List<Map.Entry<String, Class>> fields) {
        return null;
    }

    public static String groupby(String collectionName, List<Map.Entry<String, Class>> fields, List<String> operations) {
        //todo deal with operations
        Map<String, String> ops = Maps.newHashMap();
        for (String o : operations) {
            if (o.contains(": ")) {
                String[] kv = o.split(":");
                ops.put(kv[0], kv[1]);
            }

        }
        StringBuffer buffer = new StringBuffer();
        buffer.append("select ");
        List<String> x = Lists.newArrayList();
        for (Map.Entry<String, Class> kv: fields) {
            x.add(kv.getKey());
        }
        String fs = Joiner.on(',').join(x);
        buffer.append(fs);
        buffer.append(" from ");
        buffer.append(collectionName);
        buffer = addMatch(buffer, ops);
        buffer = addSort(buffer, ops);
        return buffer.toString();
    }

    private static StringBuffer addMatch(StringBuffer buffer, Map<String, String> ops) {

        return buffer;
    }

    private static StringBuffer addSort(StringBuffer buffer, Map<String, String> ops) {
        if (ops.containsKey("sort")) {
            StringBuffer newBuffer = new StringBuffer();
            newBuffer.append(ops.get("sort"));
            newBuffer.append(" ");
            newBuffer.append(buffer);
            return newBuffer;
        }
        return buffer;
    }
}
