package com.byhiras.spark.kryo;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.spark.SparkConf;
import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;

import scala.reflect.ClassTag;

/**
 * On the assumption that spark is using thread-pooling (it might not be), try and create one Kryo per thread as
 * creating them is very expensive (sometimes).
 *
 * @author Christopher Ng
 */
public class ThreadLocalKryoSerializer extends Serializer {
    private final KryoSerializer delegate;
    private final ThreadLocal<SerializerInstance> threadLocalSerializerInstance;

    public ThreadLocalKryoSerializer(SparkConf sparkConf) {
        delegate = new KryoSerializer(sparkConf);
        threadLocalSerializerInstance = new ThreadLocal<SerializerInstance>() {
            @Override
            protected SerializerInstance initialValue() {
                return delegate.newInstance();
            }
        };
    }

    @Override
    public Serializer setDefaultClassLoader(ClassLoader classLoader) {
        delegate.setDefaultClassLoader(classLoader);
        return super.setDefaultClassLoader(classLoader);
    }

    @Override
    public SerializerInstance newInstance() {
        return new ThreadLocalSerializerInstance();
    }

    private class ThreadLocalSerializerInstance extends SerializerInstance {
        private final SerializerInstance delegate = threadLocalSerializerInstance.get();
        private final Thread thread;

        ThreadLocalSerializerInstance() {
            if (ThreadLocalSerializerInstance.class.desiredAssertionStatus()) {
                thread = Thread.currentThread();
            } else {
                thread = null;
            }
        }

        @Override
        public <T> T deserialize(ByteBuffer bytes, ClassTag<T> evidence$2) {
            return delegate().deserialize(bytes, evidence$2);
        }

        @Override
        public SerializationStream serializeStream(OutputStream s) {
            return delegate().serializeStream(s);
        }

        @Override
        public <T> ByteBuffer serialize(T t, ClassTag<T> evidence$1) {
            return delegate().serialize(t, evidence$1);
        }

        @Override
        public DeserializationStream deserializeStream(InputStream s) {
            return delegate().deserializeStream(s);
        }

        @Override
        public <T> T deserialize(ByteBuffer bytes, ClassLoader loader, ClassTag<T> evidence$3) {
            return delegate().deserialize(bytes, loader, evidence$3);
        }

        private SerializerInstance delegate() {
            assert Thread.currentThread() == thread : "SerializerInstance used by a different thread.";
            return delegate;
        }
    }
}
