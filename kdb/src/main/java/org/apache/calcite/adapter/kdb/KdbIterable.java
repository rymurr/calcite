package org.apache.calcite.adapter.kdb;

import java.util.Iterator;


public class KdbIterable implements Iterable<Object[]> {
    private final c.Flip resultSet;

    public KdbIterable(c.Flip resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    public Iterator<Object[]> iterator() {
        return new KdbIterator(resultSet);
    }

    public static class KdbIterator implements Iterator<Object[]> {
        private Object[] current;
        private final c.Flip resultSet;
        private int index = 0;

        public KdbIterator(c.Flip resultSet) {
            this.resultSet = resultSet;
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

        private Object[] getCurrent() throws ArrayIndexOutOfBoundsException {
            Object[] o = new Object[resultSet.y.length];
            for (int i=0;i<resultSet.y.length;i++) {
                o[i] = ((Object[])resultSet.y[i])[index++];
            }
            return o;
        }
    }
}
