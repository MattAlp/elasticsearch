/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.materialize;

/**
 * Identifies which coordinator-owned stage is expected to perform the eventual
 * field materialization.
 */
public enum MaterializeTarget {
    CURRENT_FINAL,
    PARENT_FINAL
}
