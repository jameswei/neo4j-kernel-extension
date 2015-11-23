package com.chitu.neo4j.kernel;

import com.chitu.neo4j.handler.undertow.BinaryHandler;
import com.chitu.neo4j.handler.undertow.PingHandler;
import com.chitu.neo4j.handler.undertow.QueryHandler;
import com.chitu.neo4j.handler.undertow.WebSocketHandler;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.handlers.PathHandler;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Created by jwei on 11/20/15.
 */
public class UndertowServer implements Lifecycle, Runnable {
    private static final int DEFAULT_PORT = 8484;
    private static final String DEFAULT_ADDRESS = "127.0.0.1";

    private final GraphDatabaseService graphDB;

    private Undertow undertow;

    public UndertowServer(GraphDatabaseService graphDB) {
        this.graphDB = graphDB;
    }

    @Override
    public void init() throws Throwable {
    }

    @Override
    public void start() throws Throwable {
        new Thread(this).start();
    }

    @Override
    public void stop() throws Throwable {
    }

    @Override
    public void shutdown() throws Throwable {
        this.undertow.stop();
    }

    @Override
    public void run() {
        this.undertow = Undertow.builder()
                .setBufferSize(1024 * 16)
                .setDirectBuffers(true)
                .setIoThreads(Runtime.getRuntime().availableProcessors() * 2)
                .setWorkerThreads(Runtime.getRuntime().availableProcessors() * 2 * 8)
                .addHttpListener(DEFAULT_PORT, DEFAULT_ADDRESS)
                .setHandler(new PathHandler()
                        .addExactPath(PingHandler.HANDLE_PATH, new PingHandler())
                        .addExactPath(QueryHandler.HANDLE_PATH, new QueryHandler(graphDB))
                        .addExactPath(WebSocketHandler.HANDLE_PATH, Handlers.websocket(new WebSocketHandler(graphDB)))
                        .addPrefixPath(BinaryHandler.HANDLE_PATH, new BinaryHandler(graphDB)))
                .build();
        this.undertow.start();
        System.out.println("Undertow started...");
    }
}
