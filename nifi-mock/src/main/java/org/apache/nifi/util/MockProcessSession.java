/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.util;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.exception.FlowFileHandlingException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.provenance.ProvenanceReporter;
import org.apache.nifi.state.MockStateManager;
import org.junit.jupiter.api.Assertions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class MockProcessSession implements ProcessSession {

    private final Map<Relationship, List<MockFlowFile>> transferMap = new ConcurrentHashMap<>();
    private final MockFlowFileQueue processorQueue;
    private final Set<Long> beingProcessed = new HashSet<>();
    private final Set<Long> created = new HashSet<>();
    private final List<MockFlowFile> penalized = new ArrayList<>();
    private final Processor processor;

    private final Map<Long, MockFlowFile> currentVersions = new HashMap<>();
    private final Map<Long, MockFlowFile> originalVersions = new HashMap<>();
    private final SharedSessionState sharedState;
    private final Map<String, Long> counterMap = new HashMap<>();
    private final Map<FlowFile, Integer> readRecursionSet = new HashMap<>();
    private final Set<FlowFile> writeRecursionSet = new HashSet<>();
    private final MockProvenanceReporter provenanceReporter;
    private final boolean enforceStreamsClosed;

    // A List of InputStreams that have been created by calls to {@link #read(FlowFile)} and have not yet been closed.
    private final Map<FlowFile, InputStream> openInputStreams = new HashMap<>();
    // A List of OutputStreams that have been created by calls to {@link #write(FlowFile)} and have not yet been closed.
    private final Map<FlowFile, OutputStream> openOutputStreams = new HashMap<>();
    private final StateManager stateManager;
    private final boolean allowSynchronousCommits;

    private boolean committed = false;
    private boolean rolledBack = false;
    private final Set<Long> removedFlowFiles = new HashSet<>();

    private static final AtomicLong enqueuedIndex = new AtomicLong(0L);

    public MockProcessSession(final SharedSessionState sharedState, final Processor processor) {
        this(sharedState, processor, new MockStateManager(processor));
    }

    public MockProcessSession(final SharedSessionState sharedState, final Processor processor, final StateManager stateManager) {
        this(sharedState, processor, true, stateManager);
    }

    public MockProcessSession(final SharedSessionState sharedState, final Processor processor, final boolean enforceStreamsClosed, final StateManager stateManager) {
        this(sharedState, processor, enforceStreamsClosed, stateManager, false);
    }

    public MockProcessSession(final SharedSessionState sharedState, final Processor processor, final boolean enforceStreamsClosed, final StateManager stateManager,
                              final boolean allowSynchronousCommits) {
        this.processor = processor;
        this.enforceStreamsClosed = enforceStreamsClosed;
        this.sharedState = sharedState;
        this.processorQueue = sharedState.getFlowFileQueue();
        this.provenanceReporter = new MockProvenanceReporter(this, sharedState, processor.getIdentifier(), processor.getClass().getSimpleName());
        this.stateManager = stateManager;
        this.allowSynchronousCommits = allowSynchronousCommits;
    }

    @Override
    public void adjustCounter(final String name, final long delta, final boolean immediate) {
        if (immediate) {
            sharedState.adjustCounter(name, delta);
            return;
        }

        Long counter = counterMap.get(name);
        if (counter == null) {
            counter = delta;
            counterMap.put(name, counter);
            return;
        }

        counter = counter + delta;
        counterMap.put(name, counter);
    }

    public void migrate(final ProcessSession newOwner) {
        migrate(newOwner, List.copyOf(currentVersions.values()));
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void migrate(final ProcessSession newOwner, final Collection<FlowFile> flowFiles) {
        if (requireNonNull(newOwner) == this) {
            throw new IllegalArgumentException("Cannot migrate FlowFiles from a Process Session to itself");
        }
        if (flowFiles == null || flowFiles.isEmpty()) {
            throw new IllegalArgumentException("Must supply at least one FlowFile to migrate");
        }

        if (!(newOwner instanceof MockProcessSession)) {
            throw new IllegalArgumentException("Cannot migrate from a StandardProcessSession to a session of type " + newOwner.getClass());
        }

        migrate((MockProcessSession) newOwner, (Collection<MockFlowFile>) (Collection) flowFiles);
    }

    private void migrate(final MockProcessSession newOwner, final Collection<MockFlowFile> flowFiles) {
        for (final FlowFile flowFile : flowFiles) {
            if (openInputStreams.containsKey(flowFile)) {
                throw new IllegalStateException(flowFile + " cannot be migrated to a new Process Session because this session currently "
                        + "has an open InputStream for the FlowFile, created by calling ProcessSession.read(FlowFile)");
            }

            if (openOutputStreams.containsKey(flowFile)) {
                throw new IllegalStateException(flowFile + " cannot be migrated to a new Process Session because this session currently "
                        + "has an open OutputStream for the FlowFile, created by calling ProcessSession.write(FlowFile)");
            }

            if (readRecursionSet.containsKey(flowFile)) {
                throw new IllegalStateException(flowFile + " already in use for an active callback or InputStream created by ProcessSession.read(FlowFile) has not been closed");
            }

            if (writeRecursionSet.contains(flowFile)) {
                throw new IllegalStateException(flowFile + " already in use for an active callback or OutputStream created by ProcessSession.write(FlowFile) has not been closed");
            }

            final FlowFile currentVersion = currentVersions.get(flowFile.getId());
            if (currentVersion == null) {
                throw new FlowFileHandlingException(flowFile + " is not known in this session");
            }
        }

        for (final Map.Entry<Relationship, List<MockFlowFile>> entry : transferMap.entrySet()) {
            final Relationship relationship = entry.getKey();
            final List<MockFlowFile> transferredFlowFiles = entry.getValue();

            for (final MockFlowFile flowFile : flowFiles) {
                if (transferredFlowFiles.remove(flowFile)) {
                    newOwner.transferMap.computeIfAbsent(relationship, rel -> new ArrayList<>()).add(flowFile);
                }
            }
        }

        for (final MockFlowFile flowFile : flowFiles) {
            if (beingProcessed.remove(flowFile.getId())) {
                newOwner.beingProcessed.add(flowFile.getId());
            }

            if (penalized.remove(flowFile)) {
                newOwner.penalized.add(flowFile);
            }

            if (currentVersions.containsKey(flowFile.getId())) {
                newOwner.currentVersions.put(flowFile.getId(), currentVersions.remove(flowFile.getId()));
            }

            if (originalVersions.containsKey(flowFile.getId())) {
                newOwner.originalVersions.put(flowFile.getId(), originalVersions.remove(flowFile.getId()));
            }

            if (removedFlowFiles.remove(flowFile.getId())) {
                newOwner.removedFlowFiles.add(flowFile.getId());
            }

            if (created.remove(flowFile.getId())) {
                newOwner.created.add(flowFile.getId());
            }
        }

        final Set<String> flowFileIds = flowFiles.stream()
                .map(ff -> ff.getAttribute(CoreAttributes.UUID.key()))
                .collect(Collectors.toSet());

        provenanceReporter.migrate(newOwner.provenanceReporter, flowFileIds);
    }

    @Override
    public MockFlowFile clone(FlowFile flowFile) {
        flowFile = validateState(flowFile);
        final MockFlowFile newFlowFile = new MockFlowFile(sharedState.nextFlowFileId(), flowFile);
        updateStateWithNewFlowFile(newFlowFile);
        return newFlowFile;
    }

    @Override
    public MockFlowFile clone(FlowFile flowFile, final long offset, final long size) {
        flowFile = validateState(flowFile);
        if (offset + size > flowFile.getSize()) {
            throw new FlowFileHandlingException("Specified offset of " + offset + " and size " + size + " exceeds size of " + flowFile.toString());
        }

        final MockFlowFile newFlowFile = new MockFlowFile(sharedState.nextFlowFileId(), flowFile);
        final byte[] newContent = Arrays.copyOfRange(((MockFlowFile) flowFile).getData(), (int) offset, (int) (offset + size));
        newFlowFile.setData(newContent);

        updateStateWithNewFlowFile(newFlowFile);
        return newFlowFile;
    }

    private void closeStreams(final Map<FlowFile, ? extends Closeable> streamMap, final boolean enforceClosed) {
        final Map<FlowFile, ? extends Closeable> openStreamCopy = new HashMap<>(streamMap); // avoid ConcurrentModificationException by creating a copy of the List
        for (final Map.Entry<FlowFile, ? extends Closeable> entry : openStreamCopy.entrySet()) {
            final FlowFile flowFile = entry.getKey();
            final Closeable openStream = entry.getValue();

            try {
                openStream.close();
            } catch (IOException e) {
                throw new FlowFileAccessException("Failed to close stream for " + flowFile, e);
            }

            if (enforceClosed) {
                throw new FlowFileHandlingException("Cannot commit session because the following streams were created via "
                        + "calls to ProcessSession.read(FlowFile) or ProcessSession.write(FlowFile) and never closed: " + streamMap);
            }
        }
    }


    @Override
    public void commit() {
        if (!allowSynchronousCommits) {
            throw new RuntimeException("As of version 1.14.0, ProcessSession.commit() should be avoided when possible. See JavaDocs for explanations. Instead, use commitAsync(), " +
                    "commitAsync(Runnable), or commitAsync(Runnable, Consumer<Throwable>). However, if this is not possible, ProcessSession.commit() may still be used, but this must be explicitly " +
                    "enabled by calling TestRunner.");
        }

        commitInternal();
    }

    private void commitInternal() {
        if (!beingProcessed.isEmpty()) {
            throw new FlowFileHandlingException("Cannot commit session because the following FlowFiles have not been removed or transferred: " + beingProcessed);
        }

        closeStreams(openInputStreams, enforceStreamsClosed);
        closeStreams(openOutputStreams, enforceStreamsClosed);

        committed = true;
        beingProcessed.clear();
        currentVersions.clear();
        originalVersions.clear();
        created.clear();

        for (final Map.Entry<String, Long> entry : counterMap.entrySet()) {
            sharedState.adjustCounter(entry.getKey(), entry.getValue());
        }

        sharedState.addProvenanceEvents(provenanceReporter.getEvents());
        provenanceReporter.clear();
        counterMap.clear();
    }

    @Override
    public void commitAsync() {
        commitInternal();
    }

    @Override
    public void commitAsync(final Runnable onSuccess, final Consumer<Throwable> onFailure) {
        try {
            commitInternal();
        } catch (final Throwable t) {
            rollback();
            onFailure.accept(t);
            throw t;
        }

        onSuccess.run();
    }

    /**
     * Clear the 'committed' flag so that we can test that the next iteration of
     * {@link org.apache.nifi.processor.Processor#onTrigger} commits or rolls back the
     * session
     */
    public void clearCommitted() {
        committed = false;
    }

    /**
     * Clear the 'rolledBack' flag so that we can test that the next iteration
     * of {@link org.apache.nifi.processor.Processor#onTrigger} commits or rolls back the
     * session
     */
    public void clearRollback() {
        rolledBack = false;
    }

    @Override
    public MockFlowFile create() {
        final MockFlowFile flowFile = new MockFlowFile(sharedState.nextFlowFileId());
        updateStateWithNewFlowFile(flowFile);
        return flowFile;
    }

    @Override
    public MockFlowFile create(final FlowFile flowFile) {
        MockFlowFile newFlowFile = create();
        newFlowFile = (MockFlowFile) inheritAttributes(flowFile, newFlowFile);
        updateStateWithNewFlowFile(newFlowFile);
        return newFlowFile;
    }

    @Override
    public MockFlowFile create(final Collection<FlowFile> flowFiles) {
        MockFlowFile newFlowFile = create();
        newFlowFile = inheritAttributes(flowFiles, newFlowFile);
        updateStateWithNewFlowFile(newFlowFile);
        return newFlowFile;
    }

    @Override
    public void exportTo(FlowFile flowFile, final OutputStream out) {
        requireNonNull(out, "Argument 'out' must be non null");
        final MockFlowFile mockFlowFile = validateState(flowFile);

        try {
            out.write(mockFlowFile.getData());
        } catch (final IOException e) {
            throw new FlowFileAccessException(e.toString(), e);
        }
    }

    @Override
    public void exportTo(FlowFile flowFile, final Path path, final boolean append) {
        requireNonNull(path, "Argument 'path' must be non null");
        final MockFlowFile mockFlowFile = validateState(flowFile);

        final OpenOption mode = append ? StandardOpenOption.APPEND : StandardOpenOption.CREATE;

        try (final OutputStream out = Files.newOutputStream(path, mode)) {
            out.write(mockFlowFile.getData());
        } catch (final IOException e) {
            throw new FlowFileAccessException(e.toString(), e);
        }
    }

    @Override
    public MockFlowFile get() {
        final MockFlowFile flowFile = processorQueue.poll();
        if (flowFile != null) {
            beingProcessed.add(flowFile.getId());
            currentVersions.put(flowFile.getId(), flowFile);
            originalVersions.put(flowFile.getId(), flowFile);
        }
        return flowFile;
    }

    @Override
    public List<FlowFile> get(final int maxResults) {
        final List<FlowFile> flowFiles = new ArrayList<>(Math.min(500, maxResults));
        for (int i = 0; i < maxResults; i++) {
            final MockFlowFile nextFlowFile = get();
            if (nextFlowFile == null) {
                return flowFiles;
            }

            flowFiles.add(nextFlowFile);
        }

        return flowFiles;
    }

    @Override
    public List<FlowFile> get(final FlowFileFilter filter) {
        final List<FlowFile> flowFiles = new ArrayList<>();
        final List<MockFlowFile> unselected = new ArrayList<>();

        while (true) {
            final MockFlowFile flowFile = processorQueue.poll();
            if (flowFile == null) {
                break;
            }

            final FlowFileFilter.FlowFileFilterResult filterResult = filter.filter(flowFile);
            if (filterResult.isAccept()) {
                flowFiles.add(flowFile);

                beingProcessed.add(flowFile.getId());
                currentVersions.put(flowFile.getId(), flowFile);
                originalVersions.put(flowFile.getId(), flowFile);
            } else {
                unselected.add(flowFile);
            }

            if (!filterResult.isContinue()) {
                break;
            }
        }

        processorQueue.addAll(unselected);
        return flowFiles;
    }

    @Override
    public QueueSize getQueueSize() {
        return processorQueue.size();
    }

    @Override
    public MockFlowFile importFrom(final InputStream in, FlowFile flowFile) {
        requireNonNull(in, "Argument 'in' must be non null");
        final MockFlowFile mockFlowFile = validateState(flowFile);

        final MockFlowFile newFlowFile = new MockFlowFile(mockFlowFile.getId(), mockFlowFile);
        currentVersions.put(newFlowFile.getId(), newFlowFile);
        try {
            newFlowFile.setData(in.readAllBytes());
        } catch (final IOException e) {
            throw new FlowFileAccessException(e.toString(), e);
        }
        return newFlowFile;
    }

    @Override
    public MockFlowFile importFrom(final Path path, final boolean keepSourceFile, FlowFile flowFile) {
        requireNonNull(path, "Argument 'path' must be non null");
        final MockFlowFile mockFlowFile = validateState(flowFile);

        MockFlowFile newFlowFile = new MockFlowFile(mockFlowFile.getId(), mockFlowFile);
        currentVersions.put(newFlowFile.getId(), newFlowFile);
        newFlowFile = putAttribute(newFlowFile, CoreAttributes.FILENAME.key(), path.getFileName().toString());
        try {
            newFlowFile.setData(Files.readAllBytes(path));
        } catch (final IOException e) {
            throw new FlowFileAccessException(e.toString(), e);
        }
        return newFlowFile;
    }

    @Override
    public MockFlowFile merge(Collection<FlowFile> sources, FlowFile destination) {
        List<MockFlowFile> validatedSources = validateState(sources);
        MockFlowFile validatedDestination = validateState(destination);

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (final MockFlowFile flowFile : validatedSources) {
            try {
                outputStream.write(flowFile.getData());
            } catch (final IOException e) {
                throw new AssertionError("Failed merge FlowFile contents", e);
            }
        }

        final MockFlowFile newFlowFile = new MockFlowFile(validatedDestination.getId(), validatedDestination);
        newFlowFile.setData(outputStream.toByteArray());
        currentVersions.put(newFlowFile.getId(), newFlowFile);

        return newFlowFile;
    }

    @Override
    public MockFlowFile putAllAttributes(FlowFile flowFile, final Map<String, String> attrs) {
        requireNonNull(attrs, "Argument 'attrs' must be non null");
        final MockFlowFile mockFlowFile = validateState(flowFile);

        final MockFlowFile newFlowFile = new MockFlowFile(mockFlowFile.getId(), mockFlowFile);
        currentVersions.put(newFlowFile.getId(), newFlowFile);

        final Map<String, String> updatedAttributes;
        if (attrs.containsKey(CoreAttributes.UUID.key())) {
            updatedAttributes = new HashMap<>(attrs);
            updatedAttributes.remove(CoreAttributes.UUID.key());
        } else {
            updatedAttributes = attrs;
        }
        newFlowFile.putAttributes(updatedAttributes);
        return newFlowFile;
    }

    @Override
    public MockFlowFile putAttribute(FlowFile flowFile, final String attrName, final String attrValue) {
        requireNonNull(attrName, "Argument 'attrName' must be non null");
        requireNonNull(attrValue, "Argument 'attrValue' must be non null");
        final MockFlowFile mockFlowFile = validateState(flowFile);

        if ("uuid".equals(attrName)) {
            Assertions.fail("Should not be attempting to set FlowFile UUID via putAttribute. This will be ignored in production");
        }

        final MockFlowFile newFlowFile = new MockFlowFile(mockFlowFile.getId(), mockFlowFile);
        currentVersions.put(newFlowFile.getId(), newFlowFile);

        newFlowFile.putAttributes(Map.of(attrName, attrValue));
        return newFlowFile;
    }

    @Override
    public void read(FlowFile flowFile, final InputStreamCallback callback) {
        requireNonNull(callback, "Argument 'callback' must be non null");
        final MockFlowFile mockFlowFile = validateState(flowFile);

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(mockFlowFile.getData())) {
            incrementReadCount(mockFlowFile);
            callback.process(inputStream);
        } catch (final IOException e) {
            throw new ProcessException(e.toString(), e);
        } finally {
            decrementReadCount(mockFlowFile);
        }
    }

    private void incrementReadCount(final FlowFile flowFile) {
        readRecursionSet.compute(flowFile, (ff, count) -> count == null ? 1 : count + 1);
    }

    private void decrementReadCount(final FlowFile flowFile) {
        final Integer count = readRecursionSet.get(flowFile);
        if (count == null) {
            return;
        }

        final int updatedCount = count - 1;
        if (updatedCount == 0) {
            readRecursionSet.remove(flowFile);
        } else {
            readRecursionSet.put(flowFile, updatedCount);
        }
    }

    @Override
    public InputStream read(FlowFile flowFile) {
        requireNonNull(flowFile, "Argument 'flowFile' must be non null");

        final MockFlowFile mock = validateState(flowFile);

        final InputStream errorHandlingStream = new ByteArrayInputStream(mock.getData()) {
            @Override
            public void close() throws IOException {
                super.close();
                decrementReadCount(flowFile);
                openInputStreams.remove(mock);
            }

            @Override
            public String toString() {
                return "ErrorHandlingInputStream[flowFile=" + mock + "]";
            }
        };
        incrementReadCount(flowFile);
        openInputStreams.put(mock, errorHandlingStream);
        return errorHandlingStream;
    }

    @Override
    public void remove(FlowFile flowFile) {
        final MockFlowFile mockFlowFile = validateState(flowFile);
        final long mockFlowFileId = mockFlowFile.getId();

        final Iterator<MockFlowFile> penalizedIterator = penalized.iterator();
        while (penalizedIterator.hasNext()) {
            final MockFlowFile penalizedFlowFile = penalizedIterator.next();
            if (penalizedFlowFile.getId() == mockFlowFileId) {
                penalizedIterator.remove();
                penalized.remove(penalizedFlowFile); // TODO remove as unnecessary? - removed per iterator
                if (originalVersions.get(mockFlowFileId) != null) { // TODO remove as duplicate? see below ..
                    provenanceReporter.drop(penalizedFlowFile, penalizedFlowFile.getAttribute(CoreAttributes.DISCARD_REASON.key()));
                }
                break;
            }
        }

        final Iterator<Long> processedIterator = beingProcessed.iterator();
        while (processedIterator.hasNext()) {
            final Long processedFlowFileId = processedIterator.next();
            if (processedFlowFileId != null && processedFlowFileId == mockFlowFileId) {
                processedIterator.remove();
                beingProcessed.remove(mockFlowFileId); // TODO remove as unnecessary? - removed per iterator
                removedFlowFiles.add(mockFlowFileId);
                currentVersions.remove(mockFlowFileId);
                if (originalVersions.get(mockFlowFileId) != null) {
                    provenanceReporter.drop(mockFlowFile, mockFlowFile.getAttribute(CoreAttributes.DISCARD_REASON.key()));
                }
                return;
            }
        }

        throw new ProcessException(mockFlowFile + " not found in queue");
    }

    @Override
    public void remove(Collection<FlowFile> flowFiles) {
        for (final FlowFile flowFile : flowFiles) {
            remove(flowFile);
        }
    }

    @Override
    public MockFlowFile removeAllAttributes(FlowFile flowFile, final Set<String> attrNames) {
        requireNonNull(attrNames, "Argument 'attrNames' must be non null");
        final MockFlowFile mockFlowFile = validateState(flowFile);

        final MockFlowFile newFlowFile = new MockFlowFile(mockFlowFile.getId(), mockFlowFile);
        currentVersions.put(newFlowFile.getId(), newFlowFile);
        newFlowFile.removeAttributes(attrNames);
        return newFlowFile;
    }

    @Override
    public MockFlowFile removeAllAttributes(FlowFile flowFile, final Pattern keyPattern) {
        final MockFlowFile mockFlowFile = validateState(flowFile);
        if (keyPattern == null) {
            return mockFlowFile;
        }

        final Set<String> attrsToRemove = new HashSet<>();
        for (final String key : mockFlowFile.getAttributes().keySet()) {
            if (keyPattern.matcher(key).matches()) {
                attrsToRemove.add(key);
            }
        }

        return removeAllAttributes(mockFlowFile, attrsToRemove);
    }

    @Override
    public MockFlowFile removeAttribute(FlowFile flowFile, final String attrName) {
        requireNonNull(attrName, "Argument 'attrName' must be non null");
        final MockFlowFile mockFlowFile = validateState(flowFile);

        final MockFlowFile newFlowFile = new MockFlowFile(mockFlowFile.getId(), mockFlowFile);
        currentVersions.put(newFlowFile.getId(), newFlowFile);
        newFlowFile.removeAttributes(Set.of(attrName));
        return newFlowFile;
    }

    @Override
    public void rollback() {
        rollback(false);
    }

    @Override
    public void rollback(final boolean penalize) {
        // if we've already committed then rollback is basically a no-op
        if (committed) {
            return;
        }

        closeStreams(openInputStreams, false);
        closeStreams(openOutputStreams, false);

        for (final List<MockFlowFile> list : transferMap.values()) {
            for (final MockFlowFile flowFile : list) {
                if (!created.contains(flowFile.getId())) {
                    processorQueue.offer(flowFile);
                    if (penalize) {
                        penalized.add(flowFile);
                    }
                }
            }
        }

        for (final Long flowFileId : beingProcessed) {
            final MockFlowFile flowFile = originalVersions.get(flowFileId);
            if (flowFile != null) {
                if (!created.contains(flowFile.getId())) {
                    processorQueue.offer(flowFile);
                    if (penalize) {
                        penalized.add(flowFile);
                    }
                }
            }
        }

        rolledBack = true;
        beingProcessed.clear();
        currentVersions.clear();
        originalVersions.clear();
        transferMap.clear();
        created.clear();
        clearTransferState();
        if (!penalize) {
            penalized.clear();
        }
    }

    @Override
    public void transfer(FlowFile flowFile) {
        final MockFlowFile mockFlowFile = validateState(flowFile);

        // if the FlowFile provided was created in this session (i.e. it's in currentVersions and not in original versions),
        // then throw an exception indicating that you can't transfer FlowFiles back to self.
        // this mimics the same behavior in StandardProcessSession
        if (currentVersions.get(mockFlowFile.getId()) != null && originalVersions.get(mockFlowFile.getId()) == null) {
            throw new IllegalArgumentException("Cannot transfer FlowFiles that are created in this Session back to self");
        }

        beingProcessed.remove(mockFlowFile.getId());
        processorQueue.offer(mockFlowFile);
        updateLastQueuedDate(mockFlowFile);
    }

    private void updateLastQueuedDate(MockFlowFile mockFlowFile) {
        // Simulate StandardProcessSession.updateLastQueuedDate,
        // which is called when a flow file is transferred to a relationship.
        mockFlowFile.setLastEnqueuedDate(System.currentTimeMillis());
        mockFlowFile.setEnqueuedIndex(enqueuedIndex.incrementAndGet());
    }

    private void updateStateWithNewFlowFile(MockFlowFile newFlowFile) {
        if (newFlowFile == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        currentVersions.put(newFlowFile.getId(), newFlowFile);
        beingProcessed.add(newFlowFile.getId());
        created.add(newFlowFile.getId());
    }

    @Override
    public void transfer(final Collection<FlowFile> flowFiles) {
        for (final FlowFile flowFile : flowFiles) {
            transfer(flowFile);
        }
    }

    @Override
    public void transfer(FlowFile flowFile, final Relationship relationship) {
        if (relationship == Relationship.SELF) {
            transfer(flowFile);
            return;
        }
        if (!processor.getRelationships().contains(relationship)) {
            throw new IllegalArgumentException("this relationship " + relationship.getName() + " is not known");
        }

        final MockFlowFile mockFlowFile = validateState(flowFile);
        List<MockFlowFile> list = transferMap.computeIfAbsent(relationship, r -> new ArrayList<>());

        beingProcessed.remove(mockFlowFile.getId());
        list.add(mockFlowFile);
        updateLastQueuedDate(mockFlowFile);
    }

    @Override
    public void transfer(Collection<FlowFile> flowFiles, final Relationship relationship) {
        if (relationship == Relationship.SELF) {
            transfer(flowFiles);
            return;
        }
        for (final FlowFile flowFile : flowFiles) {
            transfer(flowFile, relationship);
        }
    }

    @Override
    public MockFlowFile write(FlowFile flowFile, final OutputStreamCallback callback) {
        requireNonNull(callback, "Argument 'callback' must be non null");
        final MockFlowFile mockFlowFile = validateState(flowFile);

        final MockFlowFile newFlowFile = new MockFlowFile(mockFlowFile.getId(), mockFlowFile);
        currentVersions.put(newFlowFile.getId(), newFlowFile);

        try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            writeRecursionSet.add(mockFlowFile);
            // todo add to open write streams?
            callback.process(outputStream);
            newFlowFile.setData(outputStream.toByteArray());
        } catch (final IOException e) {
            throw new ProcessException(e.toString(), e);
        } finally {
            // todo remove from open write streams?
            writeRecursionSet.remove(mockFlowFile);
        }

        return newFlowFile;
    }

    @Override
    public OutputStream write(FlowFile flowFile) {
        final MockFlowFile mockFlowFile = validateState(flowFile);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                super.close();
                writeRecursionSet.remove(mockFlowFile);
                openOutputStreams.remove(mockFlowFile);

                // TODO dont we need to get the most up to date version here ?
                //  final MockFlowFile currentVersion = currentVersions.get(flowFile.getId());

                final MockFlowFile newFlowFile = new MockFlowFile(mockFlowFile.getId(), mockFlowFile);
                currentVersions.put(newFlowFile.getId(), newFlowFile);
                newFlowFile.setData(toByteArray());
            }
        };
        writeRecursionSet.add(mockFlowFile);
        openOutputStreams.put(mockFlowFile, outputStream);
        return outputStream;
    }

    @Override
    public FlowFile append(FlowFile flowFile, final OutputStreamCallback callback) {
        requireNonNull(callback, "Argument 'callback' must be non null");
        final MockFlowFile mockFlowFile = validateState(flowFile);

        final MockFlowFile newFlowFile;
        try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            outputStream.write(mockFlowFile.getData());
            callback.process(outputStream);

            // TODO dont we need to get the most up to date version here ?
            //  final MockFlowFile currentVersion = currentVersions.get(flowFile.getId());
            newFlowFile = new MockFlowFile(mockFlowFile.getId(), flowFile);
            currentVersions.put(newFlowFile.getId(), newFlowFile);
            newFlowFile.setData(outputStream.toByteArray());
        } catch (final IOException e) {
            throw new ProcessException(e.toString(), e);
        }

        return newFlowFile;
    }

    @Override
    public MockFlowFile write(FlowFile flowFile, final StreamCallback callback) {
        requireNonNull(callback, "Argument 'callback' must be non null");

        return write(flowFile, (outputStream) -> {
            read(flowFile, (inputStream) -> {
                callback.process(inputStream, outputStream);
            });
        });
    }

    public List<MockFlowFile> getFlowFilesForRelationship(final Relationship relationship) {
        List<MockFlowFile> list = this.transferMap.get(relationship);
        if (list == null) {
            return List.of();
        }
        return list;
    }

    public List<MockFlowFile> getPenalizedFlowFiles() {
        return penalized;
    }

    /**
     * @param relationship to get flowfiles for
     * @return a List of FlowFiles in the order in which they were transferred
     * to the given relationship
     */
    public List<MockFlowFile> getFlowFilesForRelationship(final String relationship) {
        final Relationship procRel = new Relationship.Builder().name(relationship).build();
        return getFlowFilesForRelationship(procRel);
    }

    public MockFlowFile createFlowFile(final File file) throws IOException {
        return createFlowFile(Files.readAllBytes(file.toPath()));
    }

    public MockFlowFile createFlowFile(final byte[] data) {
        final MockFlowFile flowFile = create();
        flowFile.setData(data);
        return flowFile;
    }

    public MockFlowFile createFlowFile(final byte[] data, final Map<String, String> attrs) {
        final MockFlowFile flowFile = createFlowFile(data);
        flowFile.putAttributes(attrs);
        return flowFile;
    }

    @Override
    public MockFlowFile merge(Collection<FlowFile> sources, FlowFile destination, byte[] header, byte[] footer, byte[] demarcator) {
        final List<MockFlowFile> validatedSources = validateState(sources);
        final MockFlowFile validatedDestination = validateState(destination);

        return write(validatedDestination, (outputStream) -> {
            if (header != null) {
                outputStream.write(header);
            }

            int count = 0;
            for (final MockFlowFile flowFile : validatedSources) {
                outputStream.write(flowFile.getData());
                if (demarcator != null && ++count != validatedSources.size()) {
                    outputStream.write(demarcator);
                }
            }

            if (footer != null) {
                outputStream.write(footer);
            }
        });
    }

    private List<MockFlowFile> validateState(final Collection<FlowFile> flowFiles) {
        return flowFiles.stream().map(this::validateState).toList();
    }

    private MockFlowFile validateState(final FlowFile flowFile) {
        requireNonNull(flowFile, "Argument 'flowFile' must be non null");

        final MockFlowFile currentVersion = currentVersions.get(flowFile.getId());
        if (currentVersion == null) {
            throw new FlowFileHandlingException(flowFile + " is not known in this session");
        }

        if (readRecursionSet.containsKey(flowFile)) {
            throw new IllegalStateException(flowFile + " already in use for an active callback or InputStream created by ProcessSession.read(FlowFile) has not been closed");
        }

        if (writeRecursionSet.contains(flowFile)) {
            throw new IllegalStateException(flowFile + " already in use for an active callback or OutputStream created by ProcessSession.write(FlowFile) has not been closed");
        }

        for (final List<MockFlowFile> flowFiles : transferMap.values()) {
            if (flowFiles.contains(flowFile)) {
                throw new IllegalStateException(flowFile + " has already been transferred");
            }
        }

        return currentVersion;
    }

    /**
     * Inherits the attributes from the given source flow file into another flow
     * file. The UUID of the source becomes the parent UUID of this flow file.
     * If a parent uuid had previously been established it will be replaced by
     * the uuid of the given source
     *
     * @param source      the FlowFile from which to copy attributes
     * @param destination the FlowFile to which to copy attributes
     */
    private FlowFile inheritAttributes(final FlowFile source, final FlowFile destination) {
        if (source == null || destination == null || source == destination) {
            return destination; // don't need to inherit from ourselves
        }
        final FlowFile updated = putAllAttributes(destination, source.getAttributes());
        getProvenanceReporter().fork(source, Collections.singletonList(updated));
        return updated;
    }

    /**
     * Inherits the attributes from the given source flow files into the
     * destination flow file. The UUIDs of the sources becomes the parent UUIDs
     * of the destination flow file. Only attributes which is common to all
     * source items is copied into this flow files attributes. Any previously
     * established parent UUIDs will be replaced by the UUIDs of the sources. It
     * will capture the uuid of a certain number of source objects and may not
     * capture all of them. How many it will capture is unspecified.
     *
     * @param sources to inherit common attributes from
     */
    private MockFlowFile inheritAttributes(final Collection<FlowFile> sources, final FlowFile destination) {
        final MockFlowFile updated = putAllAttributes(destination, intersectAttributes(sources));
        getProvenanceReporter().join(sources, updated);
        return updated;
    }

    /**
     * Returns the attributes that are common to every flow file given. The key
     * and value must match exactly.
     *
     * @param flowFileList a list of flow files
     * @return the common attributes
     */
    private static Map<String, String> intersectAttributes(final Collection<FlowFile> flowFileList) {
        if (flowFileList == null || flowFileList.isEmpty()) {
            return Map.of();
        } else if (flowFileList.size() == 1) {
            return Map.copyOf(flowFileList.iterator().next().getAttributes());
        }

        /*
         * Start with the first attribute map and only put an entry to the
         * resultant map if it is common to every map.
         */
        final Map<String, String> attributesOfFirstFlowFile = flowFileList.iterator().next().getAttributes();

        return attributesOfFirstFlowFile.entrySet().stream()
                .filter(entry -> {
                    final String searchedKey = entry.getKey();
                    final String expectedValue = entry.getValue();

                    return flowFileList.stream().allMatch(flowFile -> {
                        final String otherValue = flowFile.getAttribute(searchedKey);
                        return otherValue != null && otherValue.equals(expectedValue);
                    });
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Assert that the session has been committed
     */
    public void assertCommitted() {
        Assertions.assertTrue(committed, "Session was not committed");
    }

    /**
     * Assert that the session has not been committed
     */
    public void assertNotCommitted() {
        Assertions.assertFalse(committed, "Session was committed");
    }

    /**
     * Assert that {@link #rollback()} has been called
     */
    public void assertRolledBack() {
        Assertions.assertTrue(rolledBack, "Session was not rolled back");
    }

    /**
     * Assert that {@link #rollback()} has not been called
     */
    public void assertNotRolledBack() {
        Assertions.assertFalse(rolledBack, "Session was rolled back");
    }

    /**
     * Assert that the number of FlowFiles transferred to the given relationship
     * is equal to the given count
     *
     * @param relationship to validate transfer count of
     * @param count        items transfer to given relationship
     */
    public void assertTransferCount(final Relationship relationship, final int count) {
        final int transferCount = getFlowFilesForRelationship(relationship).size();
        Assertions.assertEquals(count, transferCount, "Expected " + count + " FlowFiles to be transferred to "
                + relationship + " but actual transfer count was " + transferCount);
    }

    /**
     * Assert that the number of FlowFiles transferred to the given relationship
     * is equal to the given count
     *
     * @param relationship to validate transfer count of
     * @param count        items transfer to given relationship
     */
    public void assertTransferCount(final String relationship, final int count) {
        assertTransferCount(new Relationship.Builder().name(relationship).build(), count);
    }

    /**
     * Assert that there are no FlowFiles left on the input queue.
     */
    public void assertQueueEmpty() {
        Assertions.assertTrue(this.processorQueue.isEmpty(), "FlowFile Queue has " + this.processorQueue.size() + " items");
    }

    /**
     * Assert that at least one FlowFile is on the input queue
     */
    public void assertQueueNotEmpty() {
        Assertions.assertFalse(this.processorQueue.isEmpty(), "FlowFile Queue is empty");
    }

    /**
     * Asserts that all FlowFiles that were transferred were transferred to the
     * given relationship
     *
     * @param relationship to check for transferred flow files
     */
    public void assertAllFlowFilesTransferred(final String relationship) {
        assertAllFlowFilesTransferred(new Relationship.Builder().name(relationship).build());
    }

    /**
     * Asserts that all FlowFiles that were transferred were transferred to the
     * given relationship
     *
     * @param relationship to validate
     */
    public void assertAllFlowFilesTransferred(final Relationship relationship) {
        for (final Map.Entry<Relationship, List<MockFlowFile>> entry : transferMap.entrySet()) {
            final Relationship rel = entry.getKey();
            final List<MockFlowFile> flowFiles = entry.getValue();

            if (!rel.equals(relationship) && flowFiles != null && !flowFiles.isEmpty()) {
                Assertions.fail("Expected all Transferred FlowFiles to go to " + relationship + " but " + flowFiles.size() + " were routed to " + rel);
            }
        }
    }

    /**
     * Asserts that all FlowFiles that were transferred are compliant with the
     * given validator.
     *
     * @param validator validator to use
     */
    public void assertAllFlowFiles(FlowFileValidator validator) {
        for (final Map.Entry<Relationship, List<MockFlowFile>> entry : transferMap.entrySet()) {
            final List<MockFlowFile> flowFiles = entry.getValue();
            for (MockFlowFile mockFlowFile : flowFiles) {
                validator.assertFlowFile(mockFlowFile);
            }
        }
    }

    /**
     * Asserts that all FlowFiles that were transferred in the given relationship
     * are compliant with the given validator.
     *
     * @param validator validator to use
     */
    public void assertAllFlowFiles(Relationship relationship, FlowFileValidator validator) {
        for (final Map.Entry<Relationship, List<MockFlowFile>> entry : transferMap.entrySet()) {
            final List<MockFlowFile> flowFiles = entry.getValue();
            final Relationship rel = entry.getKey();
            for (MockFlowFile mockFlowFile : flowFiles) {
                if (rel.equals(relationship)) {
                    validator.assertFlowFile(mockFlowFile);
                }
            }
        }
    }

    /**
     * Removes all state information about FlowFiles that have been transferred
     */
    public void clearTransferState() {
        this.transferMap.clear();
    }

    /**
     * Asserts that all FlowFiles that were transferred were transferred to the
     * given relationship and that the number of FlowFiles transferred is equal
     * to <code>count</code>
     *
     * @param relationship to validate
     * @param count        number of items sent to that relationship (expected)
     */
    public void assertAllFlowFilesTransferred(final Relationship relationship, final int count) {
        assertAllFlowFilesTransferred(relationship);
        assertTransferCount(relationship, count);
    }

    /**
     * Asserts that all FlowFiles that were transferred were transferred to the
     * given relationship and that the number of FlowFiles transferred is equal
     * to <code>count</code>
     *
     * @param relationship to validate
     * @param count        number of items sent to that relationship (expected)
     */
    public void assertAllFlowFilesTransferred(final String relationship, final int count) {
        assertAllFlowFilesTransferred(new Relationship.Builder().name(relationship).build(), count);
    }

    /**
     * @return the number of FlowFiles that were removed
     */
    public int getRemovedCount() {
        return removedFlowFiles.size();
    }

    @Override
    public ProvenanceReporter getProvenanceReporter() {
        return provenanceReporter;
    }

    @Override
    public void setState(final Map<String, String> state, final Scope scope) throws IOException {
        stateManager.setState(state, scope);
    }

    @Override
    public StateMap getState(final Scope scope) throws IOException {
        return stateManager.getState(scope);
    }

    @Override
    public boolean replaceState(final StateMap oldValue, final Map<String, String> newValue, final Scope scope) throws IOException {
        return stateManager.replace(oldValue, newValue, scope);
    }

    @Override
    public void clearState(final Scope scope) throws IOException {
        stateManager.clear(scope);
    }

    @Override
    public MockFlowFile penalize(FlowFile flowFile) {
        final MockFlowFile mockFlowFile = validateState(flowFile);
        final MockFlowFile newFlowFile = new MockFlowFile(mockFlowFile.getId(), mockFlowFile);
        currentVersions.put(newFlowFile.getId(), newFlowFile);
        newFlowFile.setPenalized(true);
        penalized.add(newFlowFile);
        return newFlowFile;
    }

    public MockFlowFile unpenalize(FlowFile flowFile) {
        flowFile = validateState(flowFile);
        final MockFlowFile newFlowFile = new MockFlowFile(flowFile.getId(), flowFile);
        currentVersions.put(newFlowFile.getId(), newFlowFile);
        newFlowFile.setPenalized(false);
        penalized.remove(newFlowFile);
        return newFlowFile;
    }

    public byte[] getContentAsByteArray(MockFlowFile flowFile) {
        flowFile = validateState(flowFile);
        return flowFile.getData();
    }

    /**
     * Checks if a FlowFile is known in this session.
     *
     * @param flowFile the FlowFile to check
     * @return <code>true</code> if the FlowFile is known in this session,
     * <code>false</code> otherwise.
     */
    boolean isFlowFileKnown(final FlowFile flowFile) {
        final FlowFile currentFlowFile = currentVersions.get(flowFile.getId());
        if (currentFlowFile == null) {
            return false;
        }

        final String currentUuid = currentFlowFile.getAttribute(CoreAttributes.UUID.key());
        final String providedUuid = flowFile.getAttribute(CoreAttributes.UUID.key());
        return currentUuid.equals(providedUuid);
    }
}
