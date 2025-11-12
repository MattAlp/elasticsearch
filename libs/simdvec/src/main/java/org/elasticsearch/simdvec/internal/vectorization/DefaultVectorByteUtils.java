/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import org.elasticsearch.simdvec.VectorByteUtils;

/** Noddy implementation, you likely do not want to use this in production, check that vectorLength > 1. */
public class DefaultVectorByteUtils implements VectorByteUtils {

    public static DefaultVectorByteUtils INSTANCE = new DefaultVectorByteUtils();

    private DefaultVectorByteUtils() {}

    @Override
    public long equalMask(byte[] array, int offset, byte value) {
        return array[offset] == value ? 1L : 0L;
    }

    @Override
    public int vectorLength() {
        return 1;
    }

    @Override
    public long scanMatchingSlots(VectorizedKeySupplier keySupplier, int startSlot, int maxSlots, long targetKey) {
        if (keySupplier == null || startSlot < 0 || maxSlots <= 0) {
            return 0L;
        }

        // Fallback implementation for non-vectorized systems
        // Since vectorLength() is 1, we can only check one slot at a time
        if (maxSlots > 0 && keySupplier.getKey(startSlot) == targetKey) {
            return 1L;
        }

        return 0L;
    }
}
