package org.apache.calcite.adapter.csv;

import com.google.common.collect.Maps;
import org.apache.calcite.util.Pair;

import java.io.IOException;
import java.util.Map;

public class KdbConnection {
    private final String hostname;
    private final int port;
    private final String username;
    private final String password;
    private c conn;
    private String[] tables;
    private final Map<String, Pair<String[], char[]>> schemas= Maps.newHashMap();

    public KdbConnection(String hostname, int port, String username, String password) {
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    private c getConn(){
        if (conn == null) {
            try {
                conn = new c(hostname, port);
            } catch (c.KException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return conn;
    }

    public String[] getTables() {
        if (tables == null) {
            c conn = getConn();
            String[] tables = new String[0];
            try {
                tables = (String[]) conn.k("tables[]");
            } catch (c.KException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.tables = tables;
        }
        return tables;
    }

    public Pair<String[], char[]> getSchema(String table) {
        if (!schemas.containsKey(table)) {
            c conn = getConn();
            try {
                Object schema = conn.k("meta " + table);
                schemas.put(table, convert((c.Dict) schema));
            } catch (c.KException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return schemas.get(table);
    }

    private Pair<String[], char[]> convert(c.Dict meta) {
        c.Flip cols = (c.Flip) meta.x;
        String[] names = (String[]) cols.y[0];
        c.Flip typesMap = (c.Flip) meta.y;
        char[] types = (char[])typesMap.y[0];
        //todo deal w/ partitions etc
        return Pair.of(names, types);
    }
}

