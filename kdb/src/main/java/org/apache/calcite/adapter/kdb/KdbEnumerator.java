/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.kdb;


import org.apache.calcite.linq4j.Enumerator;

import java.util.Iterator;

/**
 * Enumerator that reads from a MongoDB collection.
 */
class KdbEnumerator implements Enumerator<Object> {
    private final Iterator<Object[]> cursor;
    private Object current;

    KdbEnumerator(Iterator<Object[]> cursor) {
        this.cursor = cursor;
    }

    public Object current() {
        return current;
    }

    public boolean moveNext() {
        try {
            if (cursor.hasNext()) {
                current = cursor.next();
                return true;
            } else {
                current = null;
                return false;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void reset() {
        throw new UnsupportedOperationException();
    }

    public void close() {
//    if (cursor instanceof KdbIterable.KdbIterator) {
//      ((KdbIterable.KdbIterator) cursor).close();
//    }
        // AggregationOutput implements Iterator but not DBCursor. There is no
        // available close() method -- apparently there is no open resource.
    }


}

// End KdbEnumerator.java
