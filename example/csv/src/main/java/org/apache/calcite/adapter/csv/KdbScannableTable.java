package org.apache.calcite.adapter.csv;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.util.Source;

public class KdbScannableTable extends KdbTable implements ScannableTable {
    public KdbScannableTable(Source source, RelProtoDataType protoRowType) {
        super(source, protoRowType);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return null;
    }
}
