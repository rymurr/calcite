package org.apache.calcite.adapter.kdb;

import com.google.common.collect.Maps;
import org.apache.calcite.avatica.util.DateTimeUtils;

import java.sql.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class KdbIterable implements Iterable<Object[]> {
    private final Object resultSet;
    private List<Map.Entry<String, Class>> fields;

    public KdbIterable(Object resultSet, List<Map.Entry<String, Class>> fields) {
        this.resultSet = resultSet;
        this.fields = fields;
    }

    @Override
    public Iterator<Object[]> iterator() {
        return new KdbIterator(resultSet, fields);
    }

    public static class KdbIterator implements Iterator<Object[]> {
        private Object[] current;
        private final Object resultSet;
        private Map<String, Integer> fields;
        private int index = 0;

        public KdbIterator(Object resultSet, List<Map.Entry<String, Class>> fields) {
            this.resultSet = resultSet;
            this.fields = Maps.newHashMap();
            int i = 0;
            for (Map.Entry<String, Class> f: fields) {
                this.fields.put(f.getKey(), i++);
            }
        }

        @Override
        public boolean hasNext() {
            try {
                current = getCurrent();
                return true;
            } catch (ArrayIndexOutOfBoundsException e) {
                return false;
            }
        }

        @Override
        public Object[] next() {
            return current;
        }

        @Override
        public void remove() {

        }

        private Object[] getFlip(c.Flip resultSet) {
            Object[] o = new Object[resultSet.y.length];
            for (int i=0;i<resultSet.y.length;i++) {
                Object val = resultSet.y[i];
                if (val instanceof double[]) {
                    o[i] = ((double[])val)[index];
                } else if (val instanceof int[]) {
                    o[i] = ((int[])val)[index];
                } else if (val instanceof long[]) {
                    o[i] = ((long[]) val)[index];
                } else if (val instanceof Date[]) {
                    Date d = ((Date[])val)[index];
                    o[i] = DateTimeUtils.dateStringToUnixDate(d.toString());
                } else {
                    o[i] = ((Object[]) resultSet.y[i])[index];
                }
            }
            return o;
        }

        private Object[] getCurrent() throws ArrayIndexOutOfBoundsException {
            if (resultSet instanceof c.Flip) {
                Object[] f = getFlip((c.Flip) resultSet);
                index++;
                return f;
            }
            if (resultSet instanceof c.Dict) {
                Object[] f = getDict((c.Dict) resultSet);
                index++;
                return f;
            }
            throw new UnsupportedOperationException("cant deal with " + resultSet.getClass().toString());
        }

        private Object[] getDict(c.Dict resultSet) {
            c.Flip keys = (c.Flip) resultSet.x;
            c.Flip values = (c.Flip) resultSet.y;
            Object[] k = getFlip(keys);
            Object[] v = getFlip(values);
            Object[] retval = new Object[fields.size()];
            int j=0;
            for (String x: keys.x) {
                int i = fields.get(x);
                retval[i] = k[j++];
            }
            j=0;
            for (String x: values.x) {
                int i = fields.get(x);
                retval[i] = v[j++];
            }
            return retval;
        }
    }
}
