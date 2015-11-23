package com.chitu.neo4j.handler.undertow;

import io.undertow.websockets.WebSocketConnectionCallback;
import io.undertow.websockets.core.*;
import io.undertow.websockets.spi.WebSocketHttpExchange;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jwei on 11/21/15.
 */
public class WebSocketHandler implements WebSocketConnectionCallback {

    public static final String HANDLE_PATH = "/undertow/websocket";
    private static final Label DIRECTOR = DynamicLabel.label("Director");

    private final GraphDatabaseService graphDB;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public WebSocketHandler(GraphDatabaseService graphDB) {
        this.graphDB = graphDB;
    }

    @Override
    public void onConnect(WebSocketHttpExchange exchange, WebSocketChannel channel) {
        channel.getReceiveSetter().set(new WebSocketChannelListener());
        channel.resumeReceives();
    }

    public class WebSocketChannelListener extends AbstractReceiveListener {
        @Override
        protected void onFullTextMessage(WebSocketChannel channel, BufferedTextMessage message) throws IOException {
            try (Transaction tx = graphDB.beginTx()) {
                Node director = graphDB.findNode(DIRECTOR, "name", "James Cameron");
                Map<String, Object> result = new HashMap<>();
                if (director != null) {
                    result.put("id", director.getId());
                    result.put("name", director.getProperty("name").toString());
                    WebSockets.sendBinary(ByteBuffer.wrap(objectMapper.writeValueAsBytes(result)), channel, null);
                } else {
                    WebSockets.sendBinary(ByteBuffer.wrap(objectMapper.writeValueAsBytes(result)), channel, null);
                }
                tx.success();
            }
        }

        @Override
        protected void onCloseMessage(CloseMessage message, WebSocketChannel channel) {
            super.onCloseMessage(message, channel);
        }
    }
}
