package org.apache.calcite.adapter.kdb;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

public class QueryGenerator {
    public static String generate(String collectionName, String filterJson, String projectJson, List<Map.Entry<String, Class>> fields) {
        return null;
    }

    public static String groupby(String collectionName, List<Map.Entry<String, Class>> fields, List<String> operations) {
        //todo deal with operations
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
        return buffer.toString();
    }
}
