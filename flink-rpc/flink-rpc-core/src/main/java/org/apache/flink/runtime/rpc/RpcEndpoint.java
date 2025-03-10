/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rpc;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ScheduledFutureAdapter;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.MdcUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for RPC endpoints. Distributed components which offer remote procedure calls have to
 * extend the RPC endpoint base class. An RPC endpoint is backed by an {@link RpcService}.
 *
 * <h1>Endpoint and Gateway</h1>
 *
 * <p>To be done...
 *
 * <h1>Single Threaded Endpoint Execution </h1>
 *
 * <p>All RPC calls on the same endpoint are called by the same thread (referred to as the
 * endpoint's <i>main thread</i>). Thus, by executing all state changing operations within the main
 * thread, we don't have to reason about concurrent accesses, in the same way in the Actor Model of
 * Erlang or Akka.
 *
 * <p>The RPC endpoint provides {@link #runAsync(Runnable)}, {@link #callAsync(Callable, Duration)}
 * and the {@link #getMainThreadExecutor()} to execute code in the RPC endpoint's main thread.
 *
 * <h1>Lifecycle</h1>
 *
 * <p>The RPC endpoint has the following stages:
 *
 * <ul>
 *   <li>The RPC endpoint is created in a non-running state and does not serve any RPC requests.
 *   <li>Calling the {@link #start()} method triggers the start of the RPC endpoint and schedules
 *       overridable {@link #onStart()} method call to the main thread.
 *   <li>When the start operation ends the RPC endpoint is moved to the running state and starts to
 *       serve and complete RPC requests.
 *   <li>Calling the {@link #closeAsync()} method triggers the termination of the RPC endpoint and
 *       schedules overridable {@link #onStop()} method call to the main thread.
 *   <li>When {@link #onStop()} method is called, it triggers an asynchronous stop operation. The
 *       RPC endpoint is not in the running state anymore but it continues to serve RPC requests.
 *   <li>When the asynchronous stop operation ends, the RPC endpoint terminates completely and does
 *       not serve RPC requests anymore.
 * </ul>
 *
 * <p>The running state can be queried in a RPC method handler or in the main thread by calling
 * {@link #isRunning()} method.
 */
public abstract class RpcEndpoint implements RpcGateway, AutoCloseableAsync {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    // ------------------------------------------------------------------------

    /** RPC service to be used to start the RPC server and to obtain rpc gateways. */
    private final RpcService rpcService;

    /** Unique identifier for this rpc endpoint. */
    private final String endpointId;

    /** Interface to access the underlying rpc server. */
    protected final RpcServer rpcServer;

    /**
     * A reference to the endpoint's main thread, if the current method is called by the main
     * thread.
     */
    final AtomicReference<Thread> currentMainThread = new AtomicReference<>(null);

    /**
     * The main thread executor to be used to execute future callbacks in the main thread of the
     * executing rpc server.
     */
    private final MainThreadExecutor mainThreadExecutor;

    /**
     * Register endpoint closeable resource to the registry and close them when the server is
     * stopped.
     */
    private final CloseableRegistry resourceRegistry;

    /**
     * Indicates whether the RPC endpoint is started and not stopped or being stopped.
     *
     * <p>IMPORTANT: the running state is not thread safe and can be used only in the main thread of
     * the rpc endpoint.
     */
    private boolean isRunning;

    /**
     * Initializes the RPC endpoint.
     *
     * @param rpcService The RPC server that dispatches calls to this RPC endpoint.
     * @param endpointId Unique identifier for this endpoint
     */
    protected RpcEndpoint(
            RpcService rpcService, String endpointId, Map<String, String> loggingContext) {
        this.rpcService = checkNotNull(rpcService, "rpcService");
        this.endpointId = checkNotNull(endpointId, "endpointId");

        this.rpcServer = rpcService.startServer(this, loggingContext);
        this.resourceRegistry = new CloseableRegistry();

        this.mainThreadExecutor =
                new MainThreadExecutor(rpcServer, this::validateRunsInMainThread, endpointId);
        registerResource(this.mainThreadExecutor);
    }

    /**
     * Initializes the RPC endpoint.
     *
     * @param rpcService The RPC server that dispatches calls to this RPC endpoint.
     * @param endpointId Unique identifier for this endpoint
     */
    protected RpcEndpoint(final RpcService rpcService, final String endpointId) {
        this(rpcService, endpointId, Collections.emptyMap());
    }

    /**
     * Initializes the RPC endpoint with a random endpoint id.
     *
     * @param rpcService The RPC server that dispatches calls to this RPC endpoint.
     */
    protected RpcEndpoint(final RpcService rpcService) {
        this(rpcService, UUID.randomUUID().toString());
    }

    /**
     * Returns the rpc endpoint's identifier.
     *
     * @return Rpc endpoint's identifier.
     */
    public String getEndpointId() {
        return endpointId;
    }

    /**
     * Returns whether the RPC endpoint is started and not stopped or being stopped.
     *
     * @return whether the RPC endpoint is started and not stopped or being stopped.
     */
    protected boolean isRunning() {
        validateRunsInMainThread();
        return isRunning;
    }

    // ------------------------------------------------------------------------
    //  Start & shutdown & lifecycle callbacks
    // ------------------------------------------------------------------------

    /**
     * Triggers start of the rpc endpoint. This tells the underlying rpc server that the rpc
     * endpoint is ready to process remote procedure calls.
     */
    public final void start() {
        rpcServer.start();
    }

    /**
     * Internal method which is called by the RpcService implementation to start the RpcEndpoint.
     *
     * @throws Exception indicating that the rpc endpoint could not be started. If an exception
     *     occurs, then the rpc endpoint will automatically terminate.
     */
    public final void internalCallOnStart() throws Exception {
        validateRunsInMainThread();
        isRunning = true;
        onStart();
    }

    /**
     * User overridable callback which is called from {@link #internalCallOnStart()}.
     *
     * <p>This method is called when the RpcEndpoint is being started. The method is guaranteed to
     * be executed in the main thread context and can be used to start the rpc endpoint in the
     * context of the rpc endpoint's main thread.
     *
     * <p>IMPORTANT: This method should never be called directly by the user.
     *
     * @throws Exception indicating that the rpc endpoint could not be started. If an exception
     *     occurs, then the rpc endpoint will automatically terminate.
     */
    protected void onStart() throws Exception {}

    /**
     * Triggers stop of the rpc endpoint. This tells the underlying rpc server that the rpc endpoint
     * is no longer ready to process remote procedure calls.
     */
    protected final void stop() {
        rpcServer.stop();
    }

    /**
     * Internal method which is called by the RpcService implementation to stop the RpcEndpoint.
     *
     * @return Future which is completed once all post stop actions are completed. If an error
     *     occurs this future is completed exceptionally
     */
    public final CompletableFuture<Void> internalCallOnStop() {
        validateRunsInMainThread();
        CompletableFuture<Void> stopFuture = new CompletableFuture<>();
        try {
            resourceRegistry.close();
            stopFuture.complete(null);
        } catch (IOException e) {
            stopFuture.completeExceptionally(
                    new RuntimeException("Close resource registry fail", e));
        }
        stopFuture = CompletableFuture.allOf(stopFuture, onStop());
        isRunning = false;
        return stopFuture;
    }

    /**
     * Register the given closeable resource to {@link CloseableRegistry}.
     *
     * @param closeableResource the given closeable resource
     */
    protected void registerResource(Closeable closeableResource) {
        try {
            resourceRegistry.registerCloseable(closeableResource);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Registry closeable resource " + closeableResource + " fail", e);
        }
    }

    /**
     * Unregister the given closeable resource from {@link CloseableRegistry}.
     *
     * @param closeableResource the given closeable resource
     * @return true if the given resource unregister successful, otherwise false
     */
    protected boolean unregisterResource(Closeable closeableResource) {
        return resourceRegistry.unregisterCloseable(closeableResource);
    }

    /**
     * User overridable callback which is called from {@link #internalCallOnStop()}.
     *
     * <p>This method is called when the RpcEndpoint is being shut down. The method is guaranteed to
     * be executed in the main thread context and can be used to clean up internal state.
     *
     * <p>IMPORTANT: This method should never be called directly by the user.
     *
     * @return Future which is completed once all post stop actions are completed. If an error
     *     occurs this future is completed exceptionally
     */
    protected CompletableFuture<Void> onStop() {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Triggers the shut down of the rpc endpoint. The shut down is executed asynchronously.
     *
     * <p>In order to wait on the completion of the shut down, obtain the termination future via
     * {@link #getTerminationFuture()}} and wait on its completion.
     */
    @Override
    public final CompletableFuture<Void> closeAsync() {
        rpcService.stopServer(rpcServer);
        return getTerminationFuture();
    }

    // ------------------------------------------------------------------------
    //  Basic RPC endpoint properties
    // ------------------------------------------------------------------------

    /**
     * Returns a self gateway of the specified type which can be used to issue asynchronous calls
     * against the RpcEndpoint.
     *
     * <p>IMPORTANT: The self gateway type must be implemented by the RpcEndpoint. Otherwise the
     * method will fail.
     *
     * @param selfGatewayType class of the self gateway type
     * @param <C> type of the self gateway to create
     * @return Self gateway of the specified type which can be used to issue asynchronous rpcs
     */
    public <C extends RpcGateway> C getSelfGateway(Class<C> selfGatewayType) {
        return rpcService.getSelfGateway(selfGatewayType, rpcServer);
    }

    /**
     * Gets the address of the underlying RPC endpoint. The address should be fully qualified so
     * that a remote system can connect to this RPC endpoint via this address.
     *
     * @return Fully qualified address of the underlying RPC endpoint
     */
    @Override
    public String getAddress() {
        return rpcServer.getAddress();
    }

    /**
     * Gets the hostname of the underlying RPC endpoint.
     *
     * @return Hostname on which the RPC endpoint is running
     */
    @Override
    public String getHostname() {
        return rpcServer.getHostname();
    }

    /**
     * Gets the main thread execution context. The main thread execution context can be used to
     * execute tasks in the main thread of the underlying RPC endpoint.
     *
     * @return Main thread execution context
     */
    protected MainThreadExecutor getMainThreadExecutor() {
        return mainThreadExecutor;
    }

    /**
     * Gets the main thread execution context. The main thread execution context can be used to
     * execute tasks in the main thread of the underlying RPC endpoint.
     *
     * @param jobID the {@link JobID} to scope the returned {@link ComponentMainThreadExecutor} to,
     *     i.e. add/remove before/after the invocations using the returned executor
     * @return Main thread execution context
     */
    protected Executor getMainThreadExecutor(JobID jobID) {
        // todo: consider caching
        return MdcUtils.scopeToJob(jobID, getMainThreadExecutor());
    }

    /**
     * Gets the endpoint's RPC service.
     *
     * @return The endpoint's RPC service
     */
    public RpcService getRpcService() {
        return rpcService;
    }

    /**
     * Return a future which is completed with true when the rpc endpoint has been terminated. In
     * case of a failure, this future is completed with the occurring exception.
     *
     * @return Future which is completed when the rpc endpoint has been terminated.
     */
    public CompletableFuture<Void> getTerminationFuture() {
        return rpcServer.getTerminationFuture();
    }

    // ------------------------------------------------------------------------
    //  Asynchronous executions
    // ------------------------------------------------------------------------

    /**
     * Execute the runnable in the main thread of the underlying RPC endpoint.
     *
     * @param runnable Runnable to be executed in the main thread of the underlying RPC endpoint
     */
    protected void runAsync(Runnable runnable) {
        rpcServer.runAsync(runnable);
    }

    /**
     * Execute the runnable in the main thread of the underlying RPC endpoint, with a delay of the
     * given number of milliseconds.
     *
     * @param runnable Runnable to be executed
     * @param delay The delay after which the runnable will be executed
     */
    protected void scheduleRunAsync(Runnable runnable, Duration delay) {
        scheduleRunAsync(runnable, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Execute the runnable in the main thread of the underlying RPC endpoint, with a delay of the
     * given number of milliseconds.
     *
     * @param runnable Runnable to be executed
     * @param delay The delay after which the runnable will be executed
     */
    protected void scheduleRunAsync(Runnable runnable, long delay, TimeUnit unit) {
        rpcServer.scheduleRunAsync(runnable, unit.toMillis(delay));
    }

    /**
     * Execute the callable in the main thread of the underlying RPC service, returning a future for
     * the result of the callable. If the callable is not completed within the given timeout, then
     * the future will be failed with a {@link TimeoutException}.
     *
     * @param callable Callable to be executed in the main thread of the underlying rpc server
     * @param timeout Timeout for the callable to be completed
     * @param <V> Return type of the callable
     * @return Future for the result of the callable.
     */
    protected <V> CompletableFuture<V> callAsync(Callable<V> callable, Duration timeout) {
        return rpcServer.callAsync(callable, timeout);
    }

    // ------------------------------------------------------------------------
    //  Main Thread Validation
    // ------------------------------------------------------------------------

    /**
     * Validates that the method call happens in the RPC endpoint's main thread.
     *
     * <p><b>IMPORTANT:</b> This check only happens when assertions are enabled, such as when
     * running tests.
     *
     * <p>This can be used for additional checks, like
     *
     * <pre>{@code
     * protected void concurrencyCriticalMethod() {
     *     validateRunsInMainThread();
     *
     *     // some critical stuff
     * }
     * }</pre>
     */
    public void validateRunsInMainThread() {
        assert MainThreadValidatorUtil.isRunningInExpectedThread(currentMainThread.get());
    }

    /**
     * Validate whether all the resources are closed.
     *
     * @return true if all the resources are closed, otherwise false
     */
    boolean validateResourceClosed() {
        return mainThreadExecutor.validateScheduledExecutorClosed() && resourceRegistry.isClosed();
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /** Executor which executes runnables in the main thread context. */
    protected static class MainThreadExecutor implements ComponentMainThreadExecutor, Closeable {
        private static final Logger log = LoggerFactory.getLogger(MainThreadExecutor.class);

        private final MainThreadExecutable gateway;
        private final Runnable mainThreadCheck;

        /**
         * The main scheduled executor manages the scheduled tasks and send them to gateway when
         * they should be executed.
         */
        private final ScheduledExecutorService mainScheduledExecutor;

        MainThreadExecutor(
                MainThreadExecutable gateway, Runnable mainThreadCheck, String endpointId) {
            this(
                    gateway,
                    mainThreadCheck,
                    Executors.newSingleThreadScheduledExecutor(
                            new ExecutorThreadFactory(endpointId + "-main-scheduler")));
        }

        @VisibleForTesting
        MainThreadExecutor(
                MainThreadExecutable gateway,
                Runnable mainThreadCheck,
                ScheduledExecutorService mainScheduledExecutor) {
            this.gateway = Preconditions.checkNotNull(gateway);
            this.mainThreadCheck = Preconditions.checkNotNull(mainThreadCheck);
            this.mainScheduledExecutor = mainScheduledExecutor;
        }

        @Override
        public void execute(@Nonnull Runnable command) {
            gateway.runAsync(command);
        }

        /**
         * The mainScheduledExecutor manages the task and sends it to the gateway after the given
         * delay.
         *
         * @param command the task to execute in the future
         * @param delay the time from now to delay the execution
         * @param unit the time unit of the delay parameter
         * @return a ScheduledFuture representing the completion of the scheduled task
         */
        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            final long delayMillis = TimeUnit.MILLISECONDS.convert(delay, unit);
            FutureTask<Void> ft = new FutureTask<>(command, null);
            if (mainScheduledExecutor.isShutdown()) {
                log.warn(
                        "The scheduled executor service is shutdown and ignores the command {}",
                        command);
            } else {
                mainScheduledExecutor.schedule(
                        () -> gateway.runAsync(ft), delayMillis, TimeUnit.MILLISECONDS);
            }
            return new ScheduledFutureAdapter<>(ft, delayMillis, TimeUnit.MILLISECONDS);
        }

        /**
         * The mainScheduledExecutor manages the given callable and sends it to the gateway after
         * the given delay.
         *
         * @param callable the callable to execute
         * @param delay the time from now to delay the execution
         * @param unit the time unit of the delay parameter
         * @param <V> result type of the callable
         * @return a ScheduledFuture which holds the future value of the given callable
         */
        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            final long delayMillis = TimeUnit.MILLISECONDS.convert(delay, unit);
            FutureTask<V> ft = new FutureTask<>(callable);
            if (mainScheduledExecutor.isShutdown()) {
                log.warn(
                        "The scheduled executor service is shutdown and ignores the callable {}",
                        callable);
            } else {
                mainScheduledExecutor.schedule(
                        () -> gateway.runAsync(ft), delayMillis, TimeUnit.MILLISECONDS);
            }
            return new ScheduledFutureAdapter<>(ft, delayMillis, TimeUnit.MILLISECONDS);
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(
                Runnable command, long initialDelay, long period, TimeUnit unit) {
            throw new UnsupportedOperationException(
                    "Not implemented because the method is currently not required.");
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(
                Runnable command, long initialDelay, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException(
                    "Not implemented because the method is currently not required.");
        }

        @Override
        public void assertRunningInMainThread() {
            mainThreadCheck.run();
        }

        /** Shutdown the {@link ScheduledThreadPoolExecutor} and remove all the pending tasks. */
        @Override
        public void close() {
            if (!mainScheduledExecutor.isShutdown()) {
                mainScheduledExecutor.shutdownNow();
            }
        }

        /**
         * Validate whether the scheduled executor is closed.
         *
         * @return true if the scheduled executor is shutdown, otherwise false
         */
        final boolean validateScheduledExecutorClosed() {
            return mainScheduledExecutor.isShutdown();
        }
    }
}
