package com.github.sparkfy.network.server;

import java.nio.ByteBuffer;

/**
 * Created by huangyu on 16/3/1.
 */
abstract public class Partitioner {

    abstract public int partition(ByteBuffer byteBuffer, int length);

}
