package com.chitu.neo4j.handler.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.websocketx.*;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jwei on 11/21/15.
 */
public class WebSocketChannelHandler extends SimpleChannelInboundHandler<Object> {
    private static final Label DIRECTOR = DynamicLabel.label("Director");
    private final GraphDatabaseService graphDB;

    private ObjectMapper objectMapper = new ObjectMapper();

    public WebSocketChannelHandler(GraphDatabaseService graphDB) {
        this.graphDB = graphDB;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof WebSocketFrame) {
            handleWebSocketFrame(ctx, (WebSocketFrame) msg);
        } else if (msg instanceof FullHttpRequest) {
            handleHttpRequest(ctx, (FullHttpRequest) msg);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    private void handleHttpRequest(ChannelHandlerContext ctx, FullHttpRequest req) {
        WebSocketServerHandshakerFactory wsFactory = new WebSocketServerHandshakerFactory(
                getWebSocketLocation(req), null, true);
        WebSocketServerHandshaker handshaker = wsFactory.newHandshaker(req);
        if (handshaker == null) {
            WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel());
        } else {
            handshaker.handshake(ctx.channel(), req);
        }
    }

    private void handleWebSocketFrame(ChannelHandlerContext ctx, WebSocketFrame frame) {
        if (frame instanceof CloseWebSocketFrame) {
            ctx.channel().write(new CloseWebSocketFrame());
        } else if (frame instanceof PingWebSocketFrame) {
            ctx.channel().write(new TextWebSocketFrame("pong"));
        } else if (frame instanceof TextWebSocketFrame) {
            try (Transaction tx = graphDB.beginTx()) {
                Node director = graphDB.findNode(DIRECTOR, "name", "James Cameron");
                Map<String, String> result = new HashMap<>();
                if (director != null) {
                    result.put("name", director.getProperty("name").toString());
                    result.put("birth_place", director.getProperty("birthplace").toString());
                    result.put("bio", director.getProperty("biography").toString());
                    ctx.channel().write(new TextWebSocketFrame(objectMapper.writeValueAsString(result)));
                } else {
                    ctx.channel().write(new BinaryWebSocketFrame());
                }
                tx.success();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }

    private static String getWebSocketLocation(FullHttpRequest req) {
        return "ws://" + req.headers().get(HttpHeaders.Names.HOST);
    }

}
