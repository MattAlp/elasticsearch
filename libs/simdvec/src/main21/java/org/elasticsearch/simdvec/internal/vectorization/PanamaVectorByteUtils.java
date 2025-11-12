/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorSpecies;

import org.elasticsearch.simdvec.VectorByteUtils;

/**
 * Utility class providing vectorized byte comparison operations using
 * the Panama Vector API (Incubator).
 *
 * <p>This class offers methods to perform efficient byte-wise comparisons
 * on arrays using SIMD instructions. It can produce bitmasks indicating
 * which bytes in a loaded block match a given value, and can also report
 * the size of the vector block being processed.</p>
 *
 * <p>Example usage:
 * <pre>{@code
 * byte[] data = ...;
 * byte target = 0x1F;
 *
 * for (int i = 0; i < data.length; i += VectorByteUtils.vectorLength()) {
 *     long mask = VectorByteUtils.equalMask(data, i, target);
 *     // Process mask...
 * }
 * }</pre>
 */
public final class PanamaVectorByteUtils implements VectorByteUtils {

    public static PanamaVectorByteUtils INSTANCE = new PanamaVectorByteUtils();

    /** The preferred byte vector species for the current platform. */
    private static final VectorSpecies<Byte> BS = ByteVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Long> LS = LongVector.SPECIES_PREFERRED;

    private PanamaVectorByteUtils() {}

    @Override
    public long equalMask(byte[] array, int offset, byte value) {
        return ByteVector.fromArray(BS, array, offset).eq(value).toLong();
    }

    @Override
    public int vectorLength() {
        return BS.length();
    }

    @Override
    public long scanMatchingSlots(VectorizedKeySupplier keySupplier, int startSlot, int maxSlots, long targetKey) {
        if (keySupplier == null || startSlot < 0 || maxSlots <= 0) {
            return 0L;
        }

        int vectorLength = LS.length();
        int slotsToCheck = Math.min(maxSlots, vectorLength);

        if (slotsToCheck == 0) {
            return 0L;
        }

        // Load candidate keys and create vector in one go
        long[] candidateKeys = new long[slotsToCheck];
        for (int i = 0; i < slotsToCheck; i++) {
            candidateKeys[i] = keySupplier.getKey(startSlot + i);
        }

        LongVector targetVector = LongVector.broadcast(LS, targetKey);
        LongVector keysVector = LongVector.fromArray(LS, candidateKeys, 0);
        var matches = keysVector.eq(targetVector);

        long resultMask = 0L;
        for (int i = 0; i < slotsToCheck; i++) {
            if (matches.laneIsSet(i)) {
                resultMask |= (1L << i);
            }
        }

        return resultMask;
    }
}
