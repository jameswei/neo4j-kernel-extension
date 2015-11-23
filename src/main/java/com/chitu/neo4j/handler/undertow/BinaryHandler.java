package com.chitu.neo4j.handler.undertow;

import com.chitu.neo4j.serialization.msgpack.MPResult;
import com.chitu.neo4j.serialization.protobuf.PBResult;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.codehaus.jackson.map.ObjectMapper;
import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.neo4j.graphdb.*;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by jwei on 11/24/15.
 */
public class BinaryHandler implements HttpHandler {
    public static final String HANDLE_PATH = "/undertow/binary/director";
    private static final Label DIRECTOR = DynamicLabel.label("Director");

    private final GraphDatabaseService graphDB;
    private final ObjectMapper objectMapper;

    public BinaryHandler(GraphDatabaseService graphDB) {
        this.graphDB = graphDB;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        if (exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }
        exchange.setStatusCode(200);
        switch (BinaryType.which(exchange.getRelativePath())) {
            case JSON: {
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
            case PROTOBUF: {
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/x-protobuf; charset=utf-8");
                try (Transaction tx = graphDB.beginTx()) {
                    ResourceIterator<Node> directors = graphDB.findNodes(DIRECTOR);
                    PBResult.Result.Builder resultBuilder = PBResult.Result.newBuilder();
                    PBResult.Item.Builder itemBuilder = PBResult.Item.newBuilder();
                    Node director;
                    while (directors.hasNext()) {
                        director = directors.next();
                        resultBuilder.addItems(itemBuilder.setId(director.getId())
                                .setName(director.getProperty("name").toString())
                                .build());
                    }
                    exchange.getResponseSender().send(ByteBuffer.wrap(resultBuilder.build().toByteArray()));
                    tx.success();
                }
            }
            case MSGPACK: {
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/x-msgpack; charset=utf-8");
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                try (Transaction tx = graphDB.beginTx()) {
                    MessagePack msgpack = new MessagePack();
                    msgpack.register(MPResult.class);
                    Packer packer = msgpack.createPacker(out);
                    ResourceIterator<Node> directors = graphDB.findNodes(DIRECTOR);
                    Node director;
                    while (directors.hasNext()) {
                        director = directors.next();
                        MPResult item = new MPResult();
                        item.id = director.getId();
                        item.name = director.getProperty("name").toString();
                        packer.write(item);
                    }
                    exchange.getResponseSender().send(ByteBuffer.wrap(out.toByteArray()));
                    tx.success();
                } finally {
                    out.close();
                }
            }
        }
    }

    private enum BinaryType {
        JSON("/json"),
        PROTOBUF("/protobuf"),
        MSGPACK("/msgpack");

        final String relativePath;

        BinaryType(String relativePath) {
            this.relativePath = relativePath;
        }

        static BinaryType which(String relativePath) {
            for (BinaryType type : BinaryType.values()) {
                if (type.relativePath.equals(relativePath)) {
                    return type;
                }
            }
            throw new IllegalArgumentException(relativePath);
        }
    }
}
