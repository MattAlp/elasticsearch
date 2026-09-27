/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.tasks.Task;

public class TaskTransportChannel implements TransportChannel {

    private final long taskId;
    private final Task task;
    private final TransportChannel channel;
    private final Releasable onTaskFinished;

    TaskTransportChannel(long taskId, TransportChannel channel, Releasable onTaskFinished) {
        this(taskId, null, channel, onTaskFinished);
    }

    TaskTransportChannel(Task task, TransportChannel channel, Releasable onTaskFinished) {
        this(task.getId(), task, channel, onTaskFinished);
    }

    private TaskTransportChannel(long taskId, Task task, TransportChannel channel, Releasable onTaskFinished) {
        this.taskId = taskId;
        this.task = task;
        this.channel = channel;
        this.onTaskFinished = onTaskFinished;
    }

    @Override
    public String getProfileName() {
        return channel.getProfileName();
    }

    @Override
    public void sendResponse(TransportResponse response) {
        try {
            channel.sendResponse(response);
        } finally {
            onTaskFinished.close();
        }
    }

    @Override
    public void sendResponse(Exception exception) {
        try {
            if (task != null) {
                task.recordTraceFailure(exception);
            }
            channel.sendResponse(exception);
        } finally {
            onTaskFinished.close();
        }
    }

    @Override
    public TransportVersion getVersion() {
        return channel.getVersion();
    }

    public TransportChannel getChannel() {
        return channel;
    }

    @Override
    public String toString() {
        return "TaskTransportChannel{task=" + taskId + "}{" + channel + "}";
    }
}
