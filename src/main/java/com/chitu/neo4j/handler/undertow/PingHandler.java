package com.chitu.neo4j.handler.undertow;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

/**
 * Created by jwei on 11/21/15.
 */
public class PingHandler implements HttpHandler {

    public static final String HANDLE_PATH = "/undertow/ping";

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        if (exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }
        exchange.setStatusCode(200);
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain; charset=utf-8");
        exchange.getResponseSender().send("pong");
    }
}
