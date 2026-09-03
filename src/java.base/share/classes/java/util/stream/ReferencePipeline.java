/*
 * Copyright (c) 2012, 2025, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */
package java.util.stream;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.DoublePredicate;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import jdk.internal.access.SharedSecrets;

/**
 * Abstract base class for an intermediate pipeline stage or pipeline source
 * stage implementing whose elements are of type {@code U}.
 *
 * @param <P_IN> type of elements in the upstream source
 * @param <P_OUT> type of elements in produced by this stage
 *
 * @since 1.8
 */
// [참조 타입 스트림의 구체적 파이프라인 구현]
//
// AbstractPipeline의 참조(객체) 타입 전문화 버전으로, Stream<T> 인터페이스를 구현한다.
// filter, map, flatMap 등 모든 참조 타입 중간 연산과
// forEach, collect, reduce 등 터미널 연산이 여기 정의된다.
// ReferencePipeline은 추상 클래스이고, 실제 stage는 주로 StatelessOp 또는 StatefulOp의 익명/구체 서브클래스
//
// 서브클래스 구조:
//   Head          — stream()을 통해 생성되는 최초 소스 스테이지
//   StatelessOp   — filter, map, flatMap, peek 등 원소 독립적 중간 연산
//   StatefulOp    — sorted, distinct, limit, skip 등 전체 원소를 봐야 하는 중간 연산
abstract class ReferencePipeline<P_IN, P_OUT>
        extends AbstractPipeline<P_IN, P_OUT, Stream<P_OUT>>
        implements Stream<P_OUT>  {

    /**
     * Constructor for the head of a stream pipeline.
     *
     * @param source {@code Supplier<Spliterator>} describing the stream source
     * @param sourceFlags the source flags for the stream source, described in
     *        {@link StreamOpFlag}
     * @param parallel {@code true} if the pipeline is parallel
     */
    ReferencePipeline(Supplier<? extends Spliterator<?>> source,
                      int sourceFlags, boolean parallel) {
        super(source, sourceFlags, parallel);
    }

    /**
     * Constructor for the head of a stream pipeline.
     *
     * @param source {@code Spliterator} describing the stream source
     * @param sourceFlags The source flags for the stream source, described in
     *        {@link StreamOpFlag}
     * @param parallel {@code true} if the pipeline is parallel
     */
    ReferencePipeline(Spliterator<?> source,
                      int sourceFlags, boolean parallel) {
        super(source, sourceFlags, parallel);
    }

    /**
     * Constructor for appending an intermediate operation onto an existing
     * pipeline.
     *
     * @param upstream the upstream element source
     * @param opFlags The operation flags for this operation, described in
     *        {@link StreamOpFlag}
     */
    ReferencePipeline(AbstractPipeline<?, P_IN, ?> upstream, int opFlags) {
        super(upstream, opFlags);
    }

     /**
      * Constructor for appending an intermediate operation onto an existing
      * pipeline.
      *
      * @param upupstream the upstream of the upstream element source
      * @param upstream the upstream element source
      * @param opFlags The operation flags for this operation, described in
      *        {@link StreamOpFlag}
      */
     protected ReferencePipeline(AbstractPipeline<?, P_IN, ?> upupstream, AbstractPipeline<?, P_IN, ?> upstream, int opFlags) {
         super(upupstream, upstream, opFlags);
     }

    // Shape-specific methods

    @Override
    final StreamShape getOutputShape() {
        return StreamShape.REFERENCE;
    }

    @Override
    final <P_IN> Node<P_OUT> evaluateToNode(PipelineHelper<P_OUT> helper,
                                        Spliterator<P_IN> spliterator,
                                        boolean flattenTree,
                                        IntFunction<P_OUT[]> generator) {
        return Nodes.collect(helper, spliterator, flattenTree, generator);
    }

    @Override
    final <P_IN> Spliterator<P_OUT> wrap(PipelineHelper<P_OUT> ph,
                                     Supplier<Spliterator<P_IN>> supplier,
                                     boolean isParallel) {
        return new StreamSpliterators.WrappingSpliterator<>(ph, supplier, isParallel);
    }

    @Override
    final Spliterator<P_OUT> lazySpliterator(Supplier<? extends Spliterator<P_OUT>> supplier) {
        return new StreamSpliterators.DelegatingSpliterator<>(supplier);
    }

    // [단락 루프 — 취소 신호가 올 때까지 원소를 하나씩 push]
    // do-while로 tryAdvance를 반복하되, sink.cancellationRequested()가 true면 즉시 중단.
    // copyInto()의 단락 경로에서 copyIntoWithCancel()을 통해 호출된다.
    @Override
    final boolean forEachWithCancel(Spliterator<P_OUT> spliterator, Sink<P_OUT> sink) {
        boolean cancelled;
        do { } while (!(cancelled = sink.cancellationRequested()) && spliterator.tryAdvance(sink));
        return cancelled;
    }

    @Override
    final Node.Builder<P_OUT> makeNodeBuilder(long exactSizeIfKnown, IntFunction<P_OUT[]> generator) {
        return Nodes.builder(exactSizeIfKnown, generator);
    }


    // BaseStream

    @Override
    public final Iterator<P_OUT> iterator() {
        return Spliterators.iterator(spliterator());
    }


    // Stream

    // Stateless intermediate operations from Stream

    @Override
    public Stream<P_OUT> unordered() {
        if (!isOrdered())
            return this;
        return new StatelessOp<>(this, StreamShape.REFERENCE, StreamOpFlag.NOT_ORDERED) {
            @Override
            Sink<P_OUT> opWrapSink(int flags, Sink<P_OUT> sink) {
                return sink;
            }
        };
    }

    // [filter — 가장 기본적인 stateless 중간 연산]
    //
    // StatelessOp 익명 서브클래스를 생성하고, opWrapSink()에서 ChainedReference를 반환한다.
    // NOT_SIZED 플래그: filter는 크기를 줄일 수 있으므로 SIZED 특성을 제거한다.
    // begin(-1): 필터 통과 후 정확한 원소 수를 알 수 없으므로 downstream에 -1을 전달.
    // accept(): predicate를 통과한 원소만 downstream으로 전달한다.
    @Override
    public final Stream<P_OUT> filter(Predicate<? super P_OUT> predicate) {
        Objects.requireNonNull(predicate);
        return new StatelessOp<>(this, StreamShape.REFERENCE,
                StreamOpFlag.NOT_SIZED) {
            @Override
            Sink<P_OUT> opWrapSink(int flags, Sink<P_OUT> sink) {
                return new Sink.ChainedReference<>(sink) {
                    @Override
                    public void begin(long size) {
                        downstream.begin(-1);  // 필터 후 크기 미지
                    }

                    @Override
                    public void accept(P_OUT u) {
                        if (predicate.test(u))   // predicate 통과 시에만 downstream으로 전달
                            downstream.accept(u);
                    }
                };
            }
        };
    }

    // [map — 원소를 변환하는 stateless 중간 연산]
    //
    // NOT_SORTED | NOT_DISTINCT: 변환 후 정렬/중복 제거 특성을 보장할 수 없으므로 제거.
    // accept(): 원소에 mapper를 적용한 결과를 downstream으로 전달. 크기는 그대로 유지(begin 오버라이드 없음).
    @Override
    public final <R> Stream<R> map(Function<? super P_OUT, ? extends R> mapper) {
        Objects.requireNonNull(mapper);
        return new StatelessOp<>(this, StreamShape.REFERENCE,
                StreamOpFlag.NOT_SORTED | StreamOpFlag.NOT_DISTINCT) {
            @Override
            Sink<P_OUT> opWrapSink(int flags, Sink<R> sink) {
                return new Sink.ChainedReference<>(sink) {
                    @Override
                    public void accept(P_OUT u) {
                        downstream.accept(mapper.apply(u));  // 변환 후 전달
                    }
                };
            }
        };
    }

    @Override
    public final IntStream mapToInt(ToIntFunction<? super P_OUT> mapper) {
        Objects.requireNonNull(mapper);
        return new IntPipeline.StatelessOp<>(this, StreamShape.REFERENCE,
                StreamOpFlag.NOT_SORTED | StreamOpFlag.NOT_DISTINCT) {
            @Override
            Sink<P_OUT> opWrapSink(int flags, Sink<Integer> sink) {
                return new Sink.ChainedReference<>(sink) {
                    @Override
                    public void accept(P_OUT u) {
                        downstream.accept(mapper.applyAsInt(u));
                    }
                };
            }
        };
    }

    @Override
    public final LongStream mapToLong(ToLongFunction<? super P_OUT> mapper) {
        Objects.requireNonNull(mapper);
        return new LongPipeline.StatelessOp<>(this, StreamShape.REFERENCE,
                StreamOpFlag.NOT_SORTED | StreamOpFlag.NOT_DISTINCT) {
            @Override
            Sink<P_OUT> opWrapSink(int flags, Sink<Long> sink) {
                return new Sink.ChainedReference<>(sink) {
                    @Override
                    public void accept(P_OUT u) {
                        downstream.accept(mapper.applyAsLong(u));
                    }
                };
            }
        };
    }

    @Override
    public final DoubleStream mapToDouble(ToDoubleFunction<? super P_OUT> mapper) {
        Objects.requireNonNull(mapper);
        return new DoublePipeline.StatelessOp<>(this, StreamShape.REFERENCE,
                StreamOpFlag.NOT_SORTED | StreamOpFlag.NOT_DISTINCT) {
            @Override
            Sink<P_OUT> opWrapSink(int flags, Sink<Double> sink) {
                return new Sink.ChainedReference<>(sink) {
                    @Override
                    public void accept(P_OUT u) {
                        downstream.accept(mapper.applyAsDouble(u));
                    }
                };
            }
        };
    }

    // [flatMap — 원소 하나를 스트림으로 확장한 후 평탄화하는 stateless 중간 연산]
    //
    // 단락 파이프라인 여부에 따라 두 가지 경로로 분기된다:
    //
    // A. 단락 없는 일반 경로 (shorts=false):
    //    result.sequential().forEach(sink) — 내부 스트림의 원소를 직접 sink로 밀어 넣음.
    //
    // B. 단락 있는 경로 (shorts=true, e.g. .flatMap(...).findFirst()):
    //    result.sequential().allMatch(this) — Predicate<R>로 FlatMap 자신을 사용.
    //    각 원소마다 sink.cancellationRequested()를 확인하여 일찍 중단 가능.
    //
    // cancel 플래그: 내부 스트림을 처리 중에도 외부 단락 신호를 전파하기 위해 사용.
    @Override
    public final <R> Stream<R> flatMap(Function<? super P_OUT, ? extends Stream<? extends R>> mapper) {
        Objects.requireNonNull(mapper);
        return new StatelessOp<>(this, StreamShape.REFERENCE,
                StreamOpFlag.NOT_SORTED | StreamOpFlag.NOT_DISTINCT | StreamOpFlag.NOT_SIZED) {
            @Override
            Sink<P_OUT> opWrapSink(int flags, Sink<R> sink) {
                boolean shorts = isShortCircuitingPipeline();  // 단락 파이프라인 여부 사전 확인
                final class FlatMap implements Sink<P_OUT>, Predicate<R> {
                    boolean cancel;

                    @Override public void begin(long size) { sink.begin(-1); }  // 평탄화 후 크기 미지
                    @Override public void end() { sink.end(); }

                    @Override
                    public void accept(P_OUT e) {
                        try (Stream<? extends R> result = mapper.apply(e)) {
                            if (result != null) {
                                if (shorts)
                                    // 단락 경로: Predicate로 자신을 넘겨 원소마다 취소 확인
                                    result.sequential().allMatch(this);
                                else
                                    // 일반 경로: 내부 스트림 원소를 모두 sink로 전달
                                    result.sequential().forEach(sink);
                            }
                        }
                    }

                    @Override
                    public boolean cancellationRequested() {
                        // 외부 sink의 취소 요청을 cancel 필드로 캐시하여 중복 폴링 방지
                        return cancel || (cancel |= sink.cancellationRequested());
                    }

                    @Override
                    public boolean test(R output) {
                        if (!cancel) {
                            sink.accept(output);
                            // 내부 원소를 보낸 후 취소 여부 확인 — false 반환 시 allMatch 중단
                            return !(cancel |= sink.cancellationRequested());
                        } else {
                            return false;
                        }
                    }
                }
                return new FlatMap();
            }
        };
    }

    @Override
    public final IntStream flatMapToInt(Function<? super P_OUT, ? extends IntStream> mapper) {
        Objects.requireNonNull(mapper);
        return new IntPipeline.StatelessOp<>(this, StreamShape.REFERENCE,
                StreamOpFlag.NOT_SORTED | StreamOpFlag.NOT_DISTINCT | StreamOpFlag.NOT_SIZED) {
            @Override
            Sink<P_OUT> opWrapSink(int flags, Sink<Integer> sink) {
                IntConsumer fastPath =
                    isShortCircuitingPipeline()
                        ? null
                        : (sink instanceof IntConsumer ic)
                            ? ic
                            : sink::accept;
                final class FlatMap implements Sink<P_OUT>, IntPredicate {
                    boolean cancel;

                    @Override public void begin(long size) { sink.begin(-1); }
                    @Override public void end() { sink.end(); }

                    @Override
                    public void accept(P_OUT e) {
                        try (IntStream result = mapper.apply(e)) {
                            if (result != null) {
                                if (fastPath == null)
                                    result.sequential().allMatch(this);
                                else
                                    result.sequential().forEach(fastPath);
                            }
                        }
                    }

                    @Override
                    public boolean cancellationRequested() {
                        return cancel || (cancel |= sink.cancellationRequested());
                    }

                    @Override
                    public boolean test(int output) {
                        if (!cancel) {
                            sink.accept(output);
                            return !(cancel |= sink.cancellationRequested());
                        } else {
                            return false;
                        }
                    }
                }
                return new FlatMap();
            }
        };
    }

    @Override
    public final DoubleStream flatMapToDouble(Function<? super P_OUT, ? extends DoubleStream> mapper) {
        Objects.requireNonNull(mapper);
        return new DoublePipeline.StatelessOp<>(this, StreamShape.REFERENCE,
                StreamOpFlag.NOT_SORTED | StreamOpFlag.NOT_DISTINCT | StreamOpFlag.NOT_SIZED) {
            @Override
            Sink<P_OUT> opWrapSink(int flags, Sink<Double> sink) {
                DoubleConsumer fastPath =
                    isShortCircuitingPipeline()
                        ? null
                        : (sink instanceof DoubleConsumer dc)
                            ? dc
                            : sink::accept;
                final class FlatMap implements Sink<P_OUT>, DoublePredicate {
                    boolean cancel;

                    @Override public void begin(long size) { sink.begin(-1); }
                    @Override public void end() { sink.end(); }

                    @Override
                    public void accept(P_OUT e) {
                        try (DoubleStream result = mapper.apply(e)) {
                            if (result != null) {
                                if (fastPath == null)
                                    result.sequential().allMatch(this);
                                else
                                    result.sequential().forEach(fastPath);
                            }
                        }
                    }

                    @Override
                    public boolean cancellationRequested() {
                        return cancel || (cancel |= sink.cancellationRequested());
                    }

                    @Override
                    public boolean test(double output) {
                        if (!cancel) {
                            sink.accept(output);
                            return !(cancel |= sink.cancellationRequested());
                        } else {
                            return false;
                        }
                    }
                }
                return new FlatMap();
            }
        };
    }

    @Override
    public final LongStream flatMapToLong(Function<? super P_OUT, ? extends LongStream> mapper) {
        Objects.requireNonNull(mapper);
        // We can do better than this, by polling cancellationRequested when stream is infinite
        return new LongPipeline.StatelessOp<>(this, StreamShape.REFERENCE,
                StreamOpFlag.NOT_SORTED | StreamOpFlag.NOT_DISTINCT | StreamOpFlag.NOT_SIZED) {
            @Override
            Sink<P_OUT> opWrapSink(int flags, Sink<Long> sink) {
                LongConsumer fastPath =
                    isShortCircuitingPipeline()
                        ? null
                        : (sink instanceof LongConsumer lc)
                            ? lc
                            : sink::accept;
                final class FlatMap implements Sink<P_OUT>, LongPredicate {
                    boolean cancel;

                    @Override public void begin(long size) { sink.begin(-1); }
                    @Override public void end() { sink.end(); }

                    @Override
                    public void accept(P_OUT e) {
                        try (LongStream result = mapper.apply(e)) {
                            if (result != null) {
                                if (fastPath == null)
                                    result.sequential().allMatch(this);
                                else
                                    result.sequential().forEach(fastPath);
                            }
                        }
                    }

                    @Override
                    public boolean cancellationRequested() {
                        return cancel || (cancel |= sink.cancellationRequested());
                    }

                    @Override
                    public boolean test(long output) {
                        if (!cancel) {
                            sink.accept(output);
                            return !(cancel |= sink.cancellationRequested());
                        } else {
                            return false;
                        }
                    }
                }
                return new FlatMap();
            }
        };
    }

    @Override
    public final <R> Stream<R> mapMulti(BiConsumer<? super P_OUT, ? super Consumer<R>> mapper) {
        Objects.requireNonNull(mapper);
        return new StatelessOp<>(this, StreamShape.REFERENCE,
                StreamOpFlag.NOT_SORTED | StreamOpFlag.NOT_DISTINCT | StreamOpFlag.NOT_SIZED) {
            @Override
            Sink<P_OUT> opWrapSink(int flags, Sink<R> sink) {
                return new Sink.ChainedReference<>(sink) {

                    @Override
                    public void begin(long size) {
                        downstream.begin(-1);
                    }

                    @Override
                    @SuppressWarnings("unchecked")
                    public void accept(P_OUT u) {
                        mapper.accept(u, (Consumer<R>) downstream);
                    }
                };
            }
        };
    }

    @Override
    public final IntStream mapMultiToInt(BiConsumer<? super P_OUT, ? super IntConsumer> mapper) {
        Objects.requireNonNull(mapper);
        return new IntPipeline.StatelessOp<>(this, StreamShape.REFERENCE,
                StreamOpFlag.NOT_SORTED | StreamOpFlag.NOT_DISTINCT | StreamOpFlag.NOT_SIZED) {
            @Override
            Sink<P_OUT> opWrapSink(int flags, Sink<Integer> sink) {
                return new Sink.ChainedReference<>(sink) {

                    @Override
                    public void begin(long size) {
                        downstream.begin(-1);
                    }

                    @Override
                    public void accept(P_OUT u) {
                        mapper.accept(u, (IntConsumer)downstream);
                    }
                };
            }
        };
    }

    @Override
    public final LongStream mapMultiToLong(BiConsumer<? super P_OUT, ? super LongConsumer> mapper) {
        Objects.requireNonNull(mapper);
        return new LongPipeline.StatelessOp<>(this, StreamShape.REFERENCE,
                StreamOpFlag.NOT_SORTED | StreamOpFlag.NOT_DISTINCT | StreamOpFlag.NOT_SIZED) {
            @Override
            Sink<P_OUT> opWrapSink(int flags, Sink<Long> sink) {
                return new Sink.ChainedReference<>(sink) {

                    @Override
                    public void begin(long size) {
                        downstream.begin(-1);
                    }

                    @Override
                    public void accept(P_OUT u) {
                        mapper.accept(u, (LongConsumer) downstream);
                    }
                };
            }
        };
    }


    @Override
    public final DoubleStream mapMultiToDouble(BiConsumer<? super P_OUT, ? super DoubleConsumer> mapper) {
        Objects.requireNonNull(mapper);
        return new DoublePipeline.StatelessOp<>(this, StreamShape.REFERENCE,
                StreamOpFlag.NOT_SORTED | StreamOpFlag.NOT_DISTINCT | StreamOpFlag.NOT_SIZED) {
            @Override
            Sink<P_OUT> opWrapSink(int flags, Sink<Double> sink) {
                return new Sink.ChainedReference<>(sink) {

                    @Override
                    public void begin(long size) {
                        downstream.begin(-1);
                    }

                    @Override
                    public void accept(P_OUT u) {
                        mapper.accept(u, (DoubleConsumer) downstream);
                    }
                };
            }
        };
    }

    @Override
    public final Stream<P_OUT> peek(Consumer<? super P_OUT> action) {
        Objects.requireNonNull(action);
        return new StatelessOp<>(this, StreamShape.REFERENCE,
                0) {
            @Override
            Sink<P_OUT> opWrapSink(int flags, Sink<P_OUT> sink) {
                return new Sink.ChainedReference<>(sink) {
                    @Override
                    public void accept(P_OUT u) {
                        action.accept(u);
                        downstream.accept(u);
                    }
                };
            }
        };
    }

    // Stateful intermediate operations from Stream

    @Override
    public final Stream<P_OUT> distinct() {
        return DistinctOps.makeRef(this);
    }

    @Override
    public final Stream<P_OUT> sorted() {
        return SortedOps.makeRef(this);
    }

    @Override
    public final Stream<P_OUT> sorted(Comparator<? super P_OUT> comparator) {
        return SortedOps.makeRef(this, comparator);
    }

    @Override
    public final Stream<P_OUT> limit(long maxSize) {
        if (maxSize < 0)
            throw new IllegalArgumentException(Long.toString(maxSize));
        return SliceOps.makeRef(this, 0, maxSize);
    }

    @Override
    public final Stream<P_OUT> skip(long n) {
        if (n < 0)
            throw new IllegalArgumentException(Long.toString(n));
        if (n == 0)
            return this;
        else
            return SliceOps.makeRef(this, n, -1);
    }

    @Override
    public final Stream<P_OUT> takeWhile(Predicate<? super P_OUT> predicate) {
        return WhileOps.makeTakeWhileRef(this, predicate);
    }

    @Override
    public final Stream<P_OUT> dropWhile(Predicate<? super P_OUT> predicate) {
        return WhileOps.makeDropWhileRef(this, predicate);
    }

    // Terminal operations from Stream

    // [forEach — 가장 단순한 터미널 연산]
    // ForEachOps.makeRef(action, false) — ordered=false이므로 병렬 시 순서 보장 없음.
    // evaluate()를 통해 파이프라인 평가를 시작한다.
    @Override
    public void forEach(Consumer<? super P_OUT> action) {
        evaluate(ForEachOps.makeRef(action, false));
    }

    @Override
    public void forEachOrdered(Consumer<? super P_OUT> action) {
        evaluate(ForEachOps.makeRef(action, true));
    }

    @Override
    @SuppressWarnings("unchecked")
    public final <A> A[] toArray(IntFunction<A[]> generator) {
        // Since A has no relation to U (not possible to declare that A is an upper bound of U)
        // there will be no static type checking.
        // Therefore use a raw type and assume A == U rather than propagating the separation of A and U
        // throughout the code-base.
        // The runtime type of U is never checked for equality with the component type of the runtime type of A[].
        // Runtime checking will be performed when an element is stored in A[], thus if A is not a
        // super type of U an ArrayStoreException will be thrown.
        @SuppressWarnings("rawtypes")
        IntFunction rawGenerator = generator;
        return (A[]) Nodes.flatten(evaluateToArrayNode(rawGenerator), rawGenerator)
                              .asArray(rawGenerator);
    }

    @Override
    public final Object[] toArray() {
        return toArray(Object[]::new);
    }

    @Override
    public List<P_OUT> toList() {
        return SharedSecrets.getJavaUtilCollectionAccess().listFromTrustedArrayNullsAllowed(this.toArray());
    }

    @Override
    public final boolean anyMatch(Predicate<? super P_OUT> predicate) {
        return evaluate(MatchOps.makeRef(predicate, MatchOps.MatchKind.ANY));
    }

    @Override
    public final boolean allMatch(Predicate<? super P_OUT> predicate) {
        return evaluate(MatchOps.makeRef(predicate, MatchOps.MatchKind.ALL));
    }

    @Override
    public final boolean noneMatch(Predicate<? super P_OUT> predicate) {
        return evaluate(MatchOps.makeRef(predicate, MatchOps.MatchKind.NONE));
    }

    @Override
    public final Optional<P_OUT> findFirst() {
        return evaluate(FindOps.makeRef(true));
    }

    @Override
    public final Optional<P_OUT> findAny() {
        return evaluate(FindOps.makeRef(false));
    }

    // [reduce — 불변 축약(immutable reduction) 터미널 연산]
    // identity(초기값) + accumulator(이항 연산) 조합.
    // 세 번째 인수로 accumulator를 combiner로도 재사용 — 병렬 시 서브태스크 결과 병합에 활용.
    @Override
    public final P_OUT reduce(final P_OUT identity, final BinaryOperator<P_OUT> accumulator) {
        return evaluate(ReduceOps.makeRef(identity, accumulator, accumulator));
    }

    @Override
    public final Optional<P_OUT> reduce(BinaryOperator<P_OUT> accumulator) {
        return evaluate(ReduceOps.makeRef(accumulator));
    }

    @Override
    public final <R> R reduce(R identity, BiFunction<R, ? super P_OUT, R> accumulator, BinaryOperator<R> combiner) {
        return evaluate(ReduceOps.makeRef(identity, accumulator, combiner));
    }

    @Override
    public final <R> Stream<R> gather(Gatherer<? super P_OUT, ?, R> gatherer) {
        return GathererOp.of(this, gatherer);
    }

    // [collect — 가변 축약(mutable reduction) 터미널 연산]
    //
    // 두 가지 경로:
    //
    // A. 병렬 + CONCURRENT + (UNORDERED 또는 비순서 파이프라인):
    //    컨테이너를 하나만 생성하고, 모든 스레드가 직접 공유하며 동시에 누적.
    //    병합(combiner) 단계가 없어 오버헤드가 적다.
    //    (예: ConcurrentHashMap을 사용하는 toConcurrentMap)
    //
    // B. 그 외 (순차 또는 일반 병렬):
    //    ReduceOps.makeRef(collector) — 각 ForkJoin 서브태스크가 독립 컨테이너를 가지고,
    //    onCompletion()에서 combiner()로 병합.
    //
    // finisher: IDENTITY_FINISH이면 컨테이너를 그대로 반환, 아니면 finisher 변환 적용.
    @Override
    @SuppressWarnings("unchecked")
    public <R, A> R collect(Collector<? super P_OUT, A, R> collector) {
        A container;
        if (isParallel()
                && (collector.characteristics().contains(Collector.Characteristics.CONCURRENT))
                && (!isOrdered() || collector.characteristics().contains(Collector.Characteristics.UNORDERED))) {
            // 경로 A: 공유 컨테이너에 직접 병렬 누적
            container = collector.supplier().get();
            BiConsumer<A, ? super P_OUT> accumulator = collector.accumulator();
            forEach(u -> accumulator.accept(container, u));
        }
        else {
            // 경로 B: 각 서브태스크 독립 컨테이너 → 병합
            container = evaluate(ReduceOps.makeRef(collector));
        }
        // IDENTITY_FINISH: finisher가 항등 변환이면 캐스팅만 수행(오버헤드 없음)
        return collector.characteristics().contains(Collector.Characteristics.IDENTITY_FINISH)
               ? (R) container
               : collector.finisher().apply(container);
    }

    @Override
    public <R> R collect(Supplier<R> supplier,
                               BiConsumer<R, ? super P_OUT> accumulator,
                               BiConsumer<R, R> combiner) {
        return evaluate(ReduceOps.makeRef(supplier, accumulator, combiner));
    }

    @Override
    public final Optional<P_OUT> max(Comparator<? super P_OUT> comparator) {
        return reduce(BinaryOperator.maxBy(comparator));
    }

    @Override
    public final Optional<P_OUT> min(Comparator<? super P_OUT> comparator) {
        return reduce(BinaryOperator.minBy(comparator));

    }

    @Override
    public final long count() {
        return evaluate(ReduceOps.makeRefCounting());
    }

    //

    /**
     * Source stage of a ReferencePipeline.
     *
     * @param <E_IN> type of elements in the upstream source
     * @param <E_OUT> type of elements in produced by this stage
     * @since 1.8
     */
    // 소스를 표현하는 Head
    // [소스 스테이지 — Collection.stream()이 반환하는 최초 파이프라인 노드]
    // Head는 opIsStateful()과 opWrapSink()를 지원하지 않는다 (UnsupportedOperationException).
    // 소스에 연산이 없을 때 forEach/forEachOrdered를 spliterator.forEachRemaining()으로 최적화한다.
    static class Head<E_IN, E_OUT> extends ReferencePipeline<E_IN, E_OUT> {
        /**
         * Constructor for the source stage of a Stream.
         *
         * @param source {@code Supplier<Spliterator>} describing the stream
         *               source
         * @param sourceFlags the source flags for the stream source, described
         *                    in {@link StreamOpFlag}
         */
        Head(Supplier<? extends Spliterator<?>> source,
             int sourceFlags, boolean parallel) {
            super(source, sourceFlags, parallel);
        }

        /**
         * Constructor for the source stage of a Stream.
         *
         * @param source {@code Spliterator} describing the stream source
         * @param sourceFlags the source flags for the stream source, described
         *                    in {@link StreamOpFlag}
         */
        Head(Spliterator<?> source,
             int sourceFlags, boolean parallel) {
            super(source, sourceFlags, parallel);
        }

        @Override
        final boolean opIsStateful() {
            throw new UnsupportedOperationException();
        }

        @Override
        final Sink<E_IN> opWrapSink(int flags, Sink<E_OUT> sink) {
            throw new UnsupportedOperationException();
        }

        // Optimized sequential terminal operations for the head of the pipeline

        @Override
        public void forEach(Consumer<? super E_OUT> action) {
            if (!isParallel()) {
                sourceStageSpliterator().forEachRemaining(action);
            }
            else {
                super.forEach(action);
            }
        }

        @Override
        public void forEachOrdered(Consumer<? super E_OUT> action) {
            if (!isParallel()) {
                sourceStageSpliterator().forEachRemaining(action);
            }
            else {
                super.forEachOrdered(action);
            }
        }
    }

    /**
     * Base class for a stateless intermediate stage of a Stream.
     *
     * @param <E_IN> type of elements in the upstream source
     * @param <E_OUT> type of elements in produced by this stage
     * @since 1.8
     */
    // [비상태(Stateless) 중간 연산의 기반 클래스]
    //
    // 각 원소를 독립적으로 처리하므로, 원소를 내부에 저장할 필요가 없다.
    // filter, map, flatMap, peek, mapToInt 등이 이 클래스를 익명 서브클래스로 확장한다.
    //
    // opIsStateful() = false이므로:
    //   - 순차/병렬 모두 파이프라인 전체를 한 번에 평가(단일 패스).
    //   - 병렬 시 Spliterator를 분할한 각 파티션이 독립적으로 싱크 체인을 실행한다.
    abstract static class StatelessOp<E_IN, E_OUT>
            extends ReferencePipeline<E_IN, E_OUT> {
        /**
         * Construct a new Stream by appending a stateless intermediate
         * operation to an existing stream.
         *
         * @param upstream The upstream pipeline stage
         * @param inputShape The stream shape for the upstream pipeline stage
         * @param opFlags Operation flags for the new stage
         */
        StatelessOp(AbstractPipeline<?, E_IN, ?> upstream,
                    StreamShape inputShape,
                    int opFlags) {
            super(upstream, opFlags);
            assert upstream.getOutputShape() == inputShape;
        }

        @Override
        final boolean opIsStateful() {
            return false;
        }
    }

    /**
     * Base class for a stateful intermediate stage of a Stream.
     *
     * @param <E_IN> type of elements in the upstream source
     * @param <E_OUT> type of elements in produced by this stage
     * @since 1.8
     */
    // [상태(Stateful) 중간 연산의 기반 클래스]
    //
    // 전체 또는 다수의 원소를 내부에 버퍼링해야 하는 연산들이 이 클래스를 확장한다.
    // sorted (전체 원소를 모아 정렬), distinct (중복 추적), limit/skip (카운터) 등.
    //
    // opIsStateful() = true의 핵심 효과:
    //   1. 순차 실행: 여전히 단일 패스지만 end()에서 버퍼 flush 가능.
    //   2. 병렬 실행: AbstractPipeline.sourceSpliterator()에서 이 연산 직전까지를
    //      먼저 평가(eager barrier)하여 결과를 새 Spliterator로 만든다.
    //      이후 단계는 그 Spliterator를 소스로 다시 시작한다.
    //   3. opEvaluateParallel()을 반드시 오버라이드해야 한다.
    abstract static class StatefulOp<E_IN, E_OUT>
            extends ReferencePipeline<E_IN, E_OUT> {
        /**
         * Construct a new Stream by appending a stateful intermediate operation
         * to an existing stream.
         * @param upstream The upstream pipeline stage
         * @param inputShape The stream shape for the upstream pipeline stage
         * @param opFlags Operation flags for the new stage
         */
        StatefulOp(AbstractPipeline<?, E_IN, ?> upstream,
                   StreamShape inputShape,
                   int opFlags) {
            super(upstream, opFlags);
            assert upstream.getOutputShape() == inputShape;
        }

        @Override
        final boolean opIsStateful() {
            return true;
        }

        @Override
        abstract <P_IN> Node<E_OUT> opEvaluateParallel(PipelineHelper<E_OUT> helper,
                                                       Spliterator<P_IN> spliterator,
                                                       IntFunction<E_OUT[]> generator);
    }
}
