package com.netflix.concurrency.limits.grpc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;

import io.grpc.MethodDescriptor.Marshaller;

public final class StringMarshaller implements Marshaller<String> {
    public static final StringMarshaller INSTANCE = new StringMarshaller();
    
    @Override
    public InputStream stream(String value) {
        return new ByteArrayInputStream(value.getBytes(Charsets.UTF_8));
    }

    @Override
    public String parse(InputStream stream) {
        try {
            return CharStreams.toString(new InputStreamReader(stream, Charsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
