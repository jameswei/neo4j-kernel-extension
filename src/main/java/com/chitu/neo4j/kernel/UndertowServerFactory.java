package com.chitu.neo4j.kernel;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Created by jwei on 11/20/15.
 */
public class UndertowServerFactory extends KernelExtensionFactory<UndertowServerFactory.Dependencies> {
    public static final String SERVICE_NAME = "undertow";

    public UndertowServerFactory() {
        super(SERVICE_NAME);
    }

    @Override
    public Lifecycle newInstance(KernelContext context, Dependencies dependencies) throws Throwable {
        return new UndertowServer(dependencies.getGraphDatabaseServie());
    }

    public interface Dependencies {
        GraphDatabaseService getGraphDatabaseServie();
    }
}
