package com.chitu.neo4j.kernel;

import com.chitu.neo4j.handler.netty.WebSocketChannelHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.lifecycle.Lifecycle;


/**
 * Created by jwei on 11/21/15.
 */
public class NettyServer implements Lifecycle, Runnable {
    private static final int DEFAULT_PORT = 8485;
    private static final String DEFAULT_ADDRESS = "127.0.0.1";

    private final GraphDatabaseService graphDB;
    private ServerBootstrap server;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public NettyServer(GraphDatabaseService graphDB) {
        this.graphDB = graphDB;
        bossGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2);
        workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2 * 8);
    }

    @Override
    public void init() throws Throwable {
        server = new ServerBootstrap();
        server.group(bossGroup, workerGroup)
                .option(ChannelOption.SO_BACKLOG, 1024).option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.MAX_MESSAGES_PER_READ, Integer.MAX_VALUE)
                .option(ChannelOption.ALLOCATOR, new PooledByteBufAllocator(true))
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new HttpServerCodec());
                        pipeline.addLast(new HttpObjectAggregator(65536));
                        pipeline.addLast(new WebSocketChannelHandler(graphDB));
                    }
                });
        final Thread runner = new Thread(this);
        runner.start();
    }

    @Override
    public void start() throws Throwable {
    }

    @Override
    public void stop() throws Throwable {
        this.bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    @Override
    public void shutdown() throws Throwable {
        this.stop();
    }

    @Override
    public void run() {
        try {
            final Channel ch = server.bind(DEFAULT_ADDRESS, DEFAULT_PORT).sync().channel();
            ch.closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
