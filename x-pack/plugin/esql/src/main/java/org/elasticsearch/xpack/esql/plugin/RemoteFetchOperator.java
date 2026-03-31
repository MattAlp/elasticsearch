/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

record RemoteFetchResult(Page inputPage, int[] groupByPosition, int[] offsetByPosition, List<List<Page>> pagesByGroup) {
    static RemoteFetchResult passthrough(Page inputPage) {
        return new RemoteFetchResult(inputPage, null, null, null);
    }

    boolean isPassthrough() {
        return groupByPosition == null;
    }
}

/**
 * Fetches deferred fields from the owning data nodes after the coordinator has narrowed the candidate set.
 */
public final class RemoteFetchOperator extends AsyncOperator<RemoteFetchResult> {
    @FunctionalInterface
    public interface Client {
        void fetchAsync(String nodeId, RemoteFetchService.Request request, ActionListener<List<Page>> listener);
    }

    public record Factory(
        int handleChannel,
        List<RemoteFetchService.FetchField> fetchFields,
        int maxOutstandingRequests,
        ThreadContext threadContext,
        Client client
    ) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new RemoteFetchOperator(driverContext, threadContext, handleChannel, fetchFields, maxOutstandingRequests, client);
        }

        @Override
        public String describe() {
            return "RemoteFetchOperator[channel=" + handleChannel + ", fields=" + fetchFields + "]";
        }
    }

    private record TargetSession(String nodeId, String sessionId) {}

    private static final class Group {
        private final TargetSession target;
        private final List<RemoteFetchHandle> handles = new ArrayList<>();

        private Group(TargetSession target) {
            this.target = target;
        }
    }

    private final DriverContext driverContext;
    private final int handleChannel;
    private final List<RemoteFetchService.FetchField> fetchFields;
    private final Client client;

    RemoteFetchOperator(
        DriverContext driverContext,
        ThreadContext threadContext,
        int handleChannel,
        List<RemoteFetchService.FetchField> fetchFields,
        int maxOutstandingRequests,
        Client client
    ) {
        super(driverContext, threadContext, maxOutstandingRequests);
        this.driverContext = driverContext;
        this.handleChannel = handleChannel;
        this.fetchFields = List.copyOf(fetchFields);
        this.client = client;
    }

    @Override
    protected void performAsync(Page inputPage, ActionListener<RemoteFetchResult> listener) {
        if (inputPage.getPositionCount() == 0 || fetchFields.isEmpty()) {
            listener.onResponse(RemoteFetchResult.passthrough(inputPage));
            return;
        }

        GroupedHandles groupedHandles = decodeHandles(inputPage);
        if (groupedHandles.groups().isEmpty()) {
            listener.onResponse(RemoteFetchResult.passthrough(inputPage));
            return;
        }

        inputPage.allowPassingToDifferentDriver();
        AtomicBoolean completed = new AtomicBoolean();
        AtomicInteger remaining = new AtomicInteger(groupedHandles.groups().size());
        List<List<Page>> pagesByGroup = new ArrayList<>(groupedHandles.groups().size());
        for (int i = 0; i < groupedHandles.groups().size(); i++) {
            pagesByGroup.add(null);
        }

        for (int groupIndex = 0; groupIndex < groupedHandles.groups().size(); groupIndex++) {
            Group group = groupedHandles.groups().get(groupIndex);
            RemoteFetchService.Request request = new RemoteFetchService.Request(group.target.sessionId(), fetchFields, group.handles);
            final int currentGroup = groupIndex;
            client.fetchAsync(group.target.nodeId(), request, ActionListener.wrap(pages -> {
                pages.forEach(Page::allowPassingToDifferentDriver);
                if (completed.get()) {
                    releasePages(pages);
                    return;
                }
                try {
                    validateFetchedPages(group, pages);
                } catch (Exception e) {
                    releasePages(pages);
                    if (completed.compareAndSet(false, true)) {
                        releasePagesByGroup(pagesByGroup);
                        listener.onFailure(e);
                    }
                    return;
                }
                pagesByGroup.set(currentGroup, pages);
                if (remaining.decrementAndGet() == 0 && completed.compareAndSet(false, true)) {
                    listener.onResponse(
                        new RemoteFetchResult(inputPage, groupedHandles.groupByPosition(), groupedHandles.offsetByPosition(), pagesByGroup)
                    );
                }
            }, e -> {
                if (completed.compareAndSet(false, true)) {
                    releasePagesByGroup(pagesByGroup);
                    listener.onFailure(e);
                }
            }));
        }
    }

    @Override
    protected void releaseFetchedOnAnyThread(RemoteFetchResult result) {
        releasePageOnAnyThread(result.inputPage());
        releasePagesByGroup(result.pagesByGroup());
    }

    @Override
    protected void doClose() {}

    @Override
    public Page getOutput() {
        RemoteFetchResult fetched = fetchFromBuffer();
        if (fetched == null) {
            return null;
        }
        if (fetched.isPassthrough()) {
            return fetched.inputPage();
        }
        return mergeFetchedPage(fetched.inputPage(), fetched.groupByPosition(), fetched.offsetByPosition(), fetched.pagesByGroup());
    }

    @Override
    public String toString() {
        return "RemoteFetchOperator[channel=" + handleChannel + ", fields=" + fetchFields + "]";
    }

    private GroupedHandles decodeHandles(Page inputPage) {
        BytesRefBlock handlesBlock = (BytesRefBlock) inputPage.getBlock(handleChannel);
        Map<TargetSession, Integer> groupLookup = new LinkedHashMap<>();
        List<Group> groups = new ArrayList<>();
        int[] groupByPosition = new int[inputPage.getPositionCount()];
        int[] offsetByPosition = new int[inputPage.getPositionCount()];
        BytesRef scratch = new BytesRef();

        for (int position = 0; position < inputPage.getPositionCount(); position++) {
            if (handlesBlock.isNull(position)) {
                throw new IllegalStateException("remote fetch handle column cannot contain nulls");
            }
            if (handlesBlock.getValueCount(position) != 1) {
                throw new IllegalStateException("remote fetch handle column must contain exactly one handle per row");
            }
            RemoteFetchHandle handle = RemoteFetchHandle.fromBytesRef(
                handlesBlock.getBytesRef(handlesBlock.getFirstValueIndex(position), scratch)
            );
            TargetSession target = new TargetSession(handle.nodeId(), handle.sessionId());
            Integer groupIndex = groupLookup.get(target);
            if (groupIndex == null) {
                groupIndex = groups.size();
                groupLookup.put(target, groupIndex);
                groups.add(new Group(target));
            }
            Group group = groups.get(groupIndex);
            groupByPosition[position] = groupIndex;
            offsetByPosition[position] = group.handles.size();
            group.handles.add(handle);
        }
        return new GroupedHandles(groups, groupByPosition, offsetByPosition);
    }

    private void validateFetchedPages(Group group, List<Page> pages) {
        int positions = 0;
        for (Page page : pages) {
            if (page.getBlockCount() != fetchFields.size()) {
                throw new IllegalStateException(
                    "remote fetch returned [" + page.getBlockCount() + "] columns but expected [" + fetchFields.size() + "]"
                );
            }
            positions += page.getPositionCount();
        }
        if (positions != group.handles.size()) {
            throw new IllegalStateException("remote fetch returned [" + positions + "] rows but expected [" + group.handles.size() + "]");
        }
    }

    private Page mergeFetchedPage(Page inputPage, int[] groupByPosition, int[] offsetByPosition, List<List<Page>> pagesByGroup) {
        Block[] outputBlocks = new Block[inputPage.getBlockCount() + fetchFields.size()];
        Block.Builder[] builders = new Block.Builder[fetchFields.size()];
        boolean success = false;
        try {
            for (int block = 0; block < inputPage.getBlockCount(); block++) {
                outputBlocks[block] = inputPage.getBlock(block);
                outputBlocks[block].incRef();
            }
            for (int field = 0; field < fetchFields.size(); field++) {
                builders[field] = PlannerUtils.toElementType(fetchFields.get(field).dataType())
                    .newBlockBuilder(inputPage.getPositionCount(), driverContext.blockFactory());
                for (int position = 0; position < inputPage.getPositionCount(); position++) {
                    List<Page> fetchedPages = pagesByGroup.get(groupByPosition[position]);
                    copyFetchedPosition(builders[field], fetchedPages, field, offsetByPosition[position]);
                }
                outputBlocks[inputPage.getBlockCount() + field] = builders[field].build();
            }
            Page output = new Page(inputPage.getPositionCount(), outputBlocks);
            success = true;
            return output;
        } finally {
            inputPage.releaseBlocks();
            releasePagesByGroup(pagesByGroup);
            Releasables.closeExpectNoException(Releasables.wrap(Arrays.asList(builders)));
            if (success == false) {
                Releasables.closeExpectNoException(Releasables.wrap(Arrays.asList(outputBlocks)));
            }
        }
    }

    private static void copyFetchedPosition(Block.Builder builder, List<Page> fetchedPages, int fieldIndex, int flattenedPosition) {
        int position = flattenedPosition;
        for (Page page : fetchedPages) {
            if (position < page.getPositionCount()) {
                builder.copyFrom(page.getBlock(fieldIndex), position, position + 1);
                return;
            }
            position -= page.getPositionCount();
        }
        throw new IllegalStateException("remote fetch response did not contain the expected row");
    }

    private static void releasePagesByGroup(List<List<Page>> pagesByGroup) {
        for (List<Page> pages : pagesByGroup) {
            releasePages(pages);
        }
    }

    private static void releasePages(List<Page> pages) {
        if (pages != null) {
            for (Page page : pages) {
                Releasables.closeExpectNoException(page::releaseBlocks);
            }
        }
    }

    private record GroupedHandles(List<Group> groups, int[] groupByPosition, int[] offsetByPosition) {}
}
