/**
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.concurrency.limits.limit.functions;

import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * Specialized utility function used by limiters to calculate thredsholds using square root
 * of the current limit.  Here we pre-compute the square root of numbers up to 1000 because
 * the square root operation can be slow.
 */
public final class SquareRootFunction implements Function<Integer, Integer> {
    static final int[] lookup = new int[1000];
    
    static {
        IntStream.range(0, 1000).forEach(i -> lookup[i] = Math.max(1, (int)Math.sqrt(i)));
    }
    
    private static final SquareRootFunction INSTANCE = new SquareRootFunction();
    
    /**
     * Create an instance of a function that returns : baseline + sqrt(limit)
     * 
     * @param baseline
     * @return
     */
    public static Function<Integer, Integer> create(int baseline) {
        return INSTANCE.andThen(t -> Math.max(baseline, t));
    }
    
    @Override
    public Integer apply(Integer t) {
        return t < 1000 ? lookup[t] : (int)Math.sqrt(t);
    }
}
