package org.apache.calcite.adapter.csv;

import org.apache.calcite.linq4j.Enumerator;

public class KdbEnumerator<T> implements Enumerator<T> {
    private final KdbConnection kdb;
    private final KdbTable table;
    private T current;
    private int index = 0;
    private final c.Flip resultSet;

    public KdbEnumerator(KdbConnection kdb, KdbTable table) {

        this.kdb = kdb;
        this.table = table;
        resultSet = kdb.select(table.source);
    }

    private Object[] getCurrent() throws ArrayIndexOutOfBoundsException {
        Object[] o = new Object[resultSet.y.length];
        for (int i=0;i<resultSet.y.length;i++) {
            o[i] = ((Object[])resultSet.y[i])[index++];
        }
        return o;
    }

    @Override
    public T current() {
        return current;
    }

    @Override
    public boolean moveNext() {
        try {
            current = (T) getCurrent();
            return true;
        } catch (ArrayIndexOutOfBoundsException e) {
            return false;
        }
    }

    @Override
    public void reset() {
        System.out.println("reset");
    }

    @Override
    public void close() {
        System.out.println("close");
    }
}
