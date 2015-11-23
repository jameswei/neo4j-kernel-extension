package com.chitu.neo4j.serialization.msgpack;

import org.msgpack.annotation.Message;

/**
 * Created by jwei on 11/23/15.
 */
@Message
public class MPResult {
    public long id;
    public String name;
}

