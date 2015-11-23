package com.chitu.neo4j.kernel;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Created by jwei on 11/21/15.
 */
public class NettyServerFactory extends KernelExtensionFactory<NettyServerFactory.Dependencies> {
    private static final String SERVICE_NAME = "netty";

    public NettyServerFactory() {
        super(SERVICE_NAME);
    }

    @Override
    public Lifecycle newInstance(KernelContext context, Dependencies dependencies) throws Throwable {
        return new NettyServer(dependencies.getGraphDatabaseService());
    }

    public interface Dependencies {
        GraphDatabaseService getGraphDatabaseService();
    }
}
