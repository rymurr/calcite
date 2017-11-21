package org.apache.calcite.adapter.kdb;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.model.JsonMapSchema;
import org.apache.calcite.util.JsonBuilder;

import java.io.IOException;
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
            if (o.contains("project&")) {
                String[] kv = o.split("&", 2);
                ops.put(kv[0], kv[1]);
                continue;
            }
            if (o.contains(": ")) {
                String[] kv = o.split(":");
                ops.put(kv[0], kv[1]);
            }

        }
        StringBuffer buffer = new StringBuffer();
        buffer.append("select ");
        buffer = addProject(buffer, ops, fields);
        buffer.append(" from ");
        buffer.append(collectionName);
        buffer = addMatch(buffer, ops);
        buffer = addSort(buffer, ops);
        return buffer.toString();
    }

    private static StringBuffer addProject(StringBuffer buffer, Map<String, String> ops, List<Map.Entry<String, Class>> fields) {
        Map<String, String> fieldMap = (ops.containsKey("project")) ? toJson(ops.get("project")) : Maps.<String, String>newHashMap();
        List<String> x = Lists.newArrayList();
        for (Map.Entry<String, Class> kv: fields) {
            if (fieldMap.containsKey(kv.getKey())) {
                x.add(fieldMap.get(kv.getKey()));
            } else {
                x.add(kv.getKey());
            }
        }
        String fs = Joiner.on(',').join(x);
        buffer.append(fs);
        return buffer;
    }

    private static Map<String, String> toJson(String project) {
        Map<String, String> map = Maps.newHashMap();
        String[] fields = project.split(",");
        for (String field: fields) {
            String[] kv = field.split(":");
            map.put(kv[0].replaceAll("\\{","").replaceAll(" ","").replaceAll("'",""), kv[1].replaceAll("}","").replaceAll(" ",""));
        }
        return map;
    }

    private static StringBuffer addMatch(StringBuffer buffer, Map<String, String> ops) {
        if (ops.containsKey("filter")){
            buffer.append(" where ");
            buffer.append(ops.get("filter"));
        }
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
