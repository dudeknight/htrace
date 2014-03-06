/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cloudera.htrace;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains the Utilities that are required for
 * async tracing especially when you are dealing Deferred objects
 *
 */
public final class AsyncTracing {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncTracing.class);

    /**
     * The callback that executes the logic for stopping the child Span and setting the
     * current Span before the start of execution of the callback chain
     * @param currentSpan
     * @param childSpan
     * @param <T>
     * @return
     */
    private static <T> Callback<T, T> createCallBack(
            final Span currentSpan, final Span childSpan) {
        return new Callback<T, T>() {
            @Override
            public T call(T t) throws Exception {
                if (Trace.currentSpan() != null && Trace.currentSpan() != childSpan) {
                    LOG.error("When we are about to stop child Span that was not the current Span");
                    LOG.error("Please check your Tracing logic");
                }
                if (childSpan != null) {
                    if (childSpan.isRunning()) {
                        childSpan.stop();
                    } else {
                        LOG.error("ChildSpan was stopped when there " +
                                "is no right for the Code to do it");
                        LOG.error("Semantically this is wrong, " +
                                "please check your Tracing logic");
                        LOG.error("Error Span " + (childSpan.toString()));
                    }
                }
                Tracer.getInstance().setCurrentSpan(currentSpan);
                return t;
            }
        };
    }

    /**
     * Wraps the original Deferred so that when the callbacks of this Deferred
     * starts executing the current Span will be the Span of the thread it is being
     * executed. After this wrapping the current Span of the thread will be a new Span created
     * in this function (only if the description is not null otherwise the new Span
     * is not created but current Span is made to null). The new Deferred because it created
     * the new Span, it will take care of stopping the child span before it sets the callbacks
     * start executing, NOTE : Contractually no other sub thread has the right to stop the child
     * Span but if it decides to it, Nothing is stopping it.
     * NOTE : No overhead what so ever when the current thread is not being traced
     * @param original
     * @param description
     * @param <T>
     * @return
     */
    public static <T> Deferred<T> wrap(Deferred<T> original, String description) {
        if (Trace.isTracing()) {
            Span currentSpan = Trace.currentSpan();
            Trace.addTimelineAnnotation(description);
            Span childSpan = null;
            if (description != null) {
                childSpan = currentSpan.child(description);
            }
            Tracer.getInstance().setCurrentSpan(childSpan);
            return original.addBoth(
                    AsyncTracing.<T>createCallBack(currentSpan, childSpan));
        } else {
            // Nothing to do as the current thread is not tracing
            return original;
        }
    }

    /**
     * Wraps the original Deferred so that when the callbacks of this Deferred
     * starts executing the current Span will be the Span of the thread it is being
     * executed. After this wrapping it will set the current Span of the thread to null
     * i.e the Thread after the execution of this function will no longer be tracing
     * @param original
     * @param <T>
     * @return
     */
    public static <T> Deferred<T> wrap(Deferred<T> original) {
        return wrap(original, null);
    }

    /**
     * Stops the current executing span, first I thought this function might be useful,
     * but after some more thinking, this function did not made sense and also may expose
     * some bugs, made it private just temporarily,
     * will remove it if no further use is seen
     **/
    private static void stop() {
        stop(null);
    }

    /**
     * Stops the current executing span, after adding a timeline annotation to the span.
     * see the stop() function comments about the reason about this
     * @param description
     */
    private static void stop(String description) {
        if (Trace.isTracing()) {
            if (description != null) {
                Trace.addTimelineAnnotation(description);
            }
            Tracer.getInstance().currentSpan().stop();
        }
    }

    /**
     * This function is synonymous with function startSpan in the Trace class,
     * but sometimes due to some bug or some edge cases the span may not get stopped,
     * but still you may want to start a new ROOT Span and also log the old span may be
     * for debugging purposes.
     *
     * NOTE: you have call STOP on this returned TraceScope when you have finished
     * what ever you have decided to finish the part of the function that created this
     *
     * @param description
     */
    public static TraceScope startRootSpan(String description, Sampler<Object> sampler) {
        if (description == null) {
            LOG.error("Common bro you are testing patience here");
            LOG.error("Failing fast here");
        }

        if (Trace.isTracing()) {
            LOG.error("Should not be tracing while you try start ROOT Span");
            LOG.error("Please Check your tracing logic");
            LOG.error("Span that was present " + Tracer.getInstance().currentSpan());
            Tracer.getInstance().setCurrentSpan(null);
        }
        // The current thread will no tracing at this point
        // so you can call the create new only if the sampler allows
        if (sampler.next(null)) {
            Tracer tracer = Tracer.getInstance();
            return tracer.continueSpan(tracer.createNew(description));
        } else {
            return null;
        }
    }
}
