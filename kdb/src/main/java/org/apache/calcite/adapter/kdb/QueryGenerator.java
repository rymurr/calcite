package org.apache.calcite.adapter.kdb;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.model.JsonMapSchema;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Pair;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
            if (o.contains("group$")) {
                String[] kv = o.split("\\$", 2);
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
        Pair<String, List<String>> pair = addGroup(ops, fields);
        buffer = addProject(buffer, ops, pair.right, pair.left);
        buffer.append(" from ");
        buffer.append(collectionName);
        buffer = addMatch(buffer, ops);
        buffer = addSort(buffer, ops);
        buffer = addLimit(buffer, ops);
        String query = buffer.toString();
        /*if (pair.left != null) {
            query = "0!" + query;
        }*/
        return query;
    }

    private static StringBuffer addLimit(StringBuffer buffer, Map<String, String> ops) {
        if (ops.containsKey("limit")) {
            StringBuffer newBuffer = new StringBuffer();
            newBuffer.append("select from (");
            newBuffer.append(buffer);
            newBuffer.append(" ) where ");
            newBuffer.append(ops.get("limit"));
            return newBuffer;
        }
        return buffer;
    }

    private static Pair<String, List<String>> addGroup(Map<String, String> ops, List<Map.Entry<String, Class>> fields) {
        List<String> badFields = Lists.newArrayList();
        if (ops.containsKey("group")) {
            String grp = ops.get("group");
            Set<String> arrFields = Sets.newHashSet(grp.replaceAll(":","").split(" "));
            for (Map.Entry<String, Class> field : fields) {
                if (arrFields.contains(field.getKey())) {
                    badFields.add(field.getKey());
                }
            }
            return new Pair<>(grp, badFields);
        }
        return new Pair<>(null, badFields);
    }

    private static StringBuffer addProject(StringBuffer buffer, Map<String, String> ops, List<String> groupFields, String groupbyStatement) {
        if (ops.containsKey("project")) {
            String[] keys = ops.get("project").split(",");
            List<String> fields = Lists.newArrayList();
            Set<String> fieldSet = Sets.newHashSet(groupFields);
            for (String key: keys) {
                if (!fieldSet.contains(key.replaceAll(" ",""))) {
                    fields.add(key);
                }
            }
            if (groupbyStatement != null) {
                fields.add(groupbyStatement);
            }
            buffer.append(" ");
            buffer.append(Joiner.on(",").join(fields));
            buffer.append(" ");
        } else {
            List<String> fields = Lists.newArrayList();
            if (groupbyStatement != null) {
                fields.add(groupbyStatement);
            }
            buffer.append(" ");
            buffer.append(Joiner.on(",").join(fields));
            buffer.append(" ");
        }
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
