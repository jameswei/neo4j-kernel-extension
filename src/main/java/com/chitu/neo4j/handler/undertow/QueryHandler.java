package com.chitu.neo4j.handler.undertow;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.*;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by jwei on 11/21/15.
 */
public class QueryHandler implements HttpHandler {

    public static final String HANDLE_PATH = "/undertow/director";
    private static final Label DIRECTOR = DynamicLabel.label("Director");

    private final GraphDatabaseService graphDB;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public QueryHandler(GraphDatabaseService graphDB) {
        this.graphDB = graphDB;
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        if (exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }
        exchange.setStatusCode(200);
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json; charset=utf-8");
        try (Transaction tx = graphDB.beginTx()) {
            ResourceIterator<Node> directors = graphDB.findNodes(DIRECTOR);
            Node director;
            List<Map<String, Object>> result = new LinkedList<>();
            Map<String, Object> item;
            while (directors.hasNext()) {
                director = directors.next();
                item = new HashMap<>();
                item.put("id", director.getId());
                item.put("name", director.getProperty("name").toString());
                result.add(item);
            }
            exchange.getResponseSender().send(ByteBuffer.wrap(objectMapper.writeValueAsBytes(result)));
            tx.success();
        }
    }
}
