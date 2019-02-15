package com.netflix.concurrency.limits.grpc.mockito;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Optional;

public class OptionalResultCaptor<T> implements Answer<Optional<T>> {

    public static <T> OptionalResultCaptor<T> forClass(Class<T> type) {
        return new OptionalResultCaptor<T>();
    }

    private Optional<T> result = null;

    public Optional<T> getResult() {
        return result;
    }

    @Override
    public Optional<T> answer(InvocationOnMock invocationOnMock) throws Throwable {
        result = (Optional<T>)invocationOnMock.callRealMethod();
        if (result.isPresent()) {
            result = result.map(Mockito::spy);
        }
        return result;
    }
}
