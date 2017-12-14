package com.netflix.concurrency.limits.internal;

public final class Preconditions {
    public static void checkArgument(boolean expression, Object errorMessage) {
        if (!expression) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }

    private Preconditions() {
    }
}
