package org.apache.calcite.adapter.kdb;

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
        private List<Map.Entry<String, Class>> fields;
        private int index = 0;

        public KdbIterator(Object resultSet, List<Map.Entry<String, Class>> fields) {
            this.resultSet = resultSet;
            this.fields = fields;
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
            index++;
            return o;
        }

        private Object[] getCurrent() throws ArrayIndexOutOfBoundsException {
            if (resultSet instanceof c.Flip) {
                return getFlip((c.Flip) resultSet);
            }
            if (resultSet instanceof c.Dict) {
                return null; //todo
            }
            throw new UnsupportedOperationException("cant deal with " + resultSet.getClass().toString());
        }
    }
}
