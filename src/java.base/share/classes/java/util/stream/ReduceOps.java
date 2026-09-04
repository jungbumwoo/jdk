/*
 * Copyright (c) 2012, 2021, Oracle and/or its affiliates. All rights reserved.
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

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Spliterator;
import java.util.concurrent.CountedCompleter;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.DoubleBinaryOperator;
import java.util.function.IntBinaryOperator;
import java.util.function.LongBinaryOperator;
import java.util.function.ObjDoubleConsumer;
import java.util.function.ObjIntConsumer;
import java.util.function.ObjLongConsumer;
import java.util.function.Supplier;

/**
 * Factory for creating instances of {@code TerminalOp} that implement
 * reductions.
 *
 * @since 1.8
 */
// [축약(Reduction) 터미널 연산 팩토리]
//
// reduce(), collect(), count() 등 다양한 축약 연산을 위한 TerminalOp 인스턴스를 생성한다.
// 내부적으로 ReduceOp(추상 클래스) + AccumulatingSink 패턴을 사용한다.
//
// 핵심 추상화: AccumulatingSink
//   - begin():   누산기(accumulator) 초기화
//   - accept():  원소를 누산기에 반영
//   - combine(): 두 부분 결과를 병합 — 병렬 스트림의 ForkJoin 병합에 사용
//   - get():     최종 결과 반환
//
// 병렬 실행: ReduceTask(ForkJoinTask 서브클래스)가 Spliterator를 재귀적으로 분할하고,
//   각 리프 태스크가 독립 ReducingSink로 부분 결과를 계산한 후
//   onCompletion()에서 leftResult.combine(rightResult)로 트리 형태로 병합한다.
final class ReduceOps {

    private ReduceOps() { }

    /**
     * Constructs a {@code TerminalOp} that implements a functional reduce on
     * reference values.
     *
     * @param <T> the type of the input elements
     * @param <U> the type of the result
     * @param seed the identity element for the reduction
     * @param reducer the accumulating function that incorporates an additional
     *        input element into the result
     * @param combiner the combining function that combines two intermediate
     *        results
     * @return a {@code TerminalOp} implementing the reduction
     */
    // [identity + BiFunction 버전 reduce — Stream.reduce(identity, accumulator, combiner)]
    //
    // ReducingSink 내부 동작:
    //   begin():   state = seed (identity값으로 초기화)
    //   accept():  state = reducer.apply(state, t) (원소마다 누적)
    //   combine(): state = combiner.apply(state, other.state) (병렬 병합)
    //
    // evaluateSequential: wrapAndCopyInto(sink, spliterator) → sink.get()
    // evaluateParallel:   new ReduceTask(...).invoke() → ForkJoin으로 분할 후 combine
    public static <T, U> TerminalOp<T, U>
    makeRef(U seed, BiFunction<U, ? super T, U> reducer, BinaryOperator<U> combiner) {
        Objects.requireNonNull(reducer);
        Objects.requireNonNull(combiner);
        class ReducingSink extends Box<U> implements AccumulatingSink<T, U, ReducingSink> {
            @Override
            public void begin(long size) {
                state = seed;   // 항등원(identity)으로 초기화
            }

            @Override
            public void accept(T t) {
                state = reducer.apply(state, t);   // 원소마다 현재 상태에 누적
            }

            @Override
            public void combine(ReducingSink other) {
                // 병렬 서브태스크 결과 병합: ForkJoin onCompletion에서 호출됨
                state = combiner.apply(state, other.state);
            }
        }
        return new ReduceOp<T, U, ReducingSink>(StreamShape.REFERENCE) {
            @Override
            public ReducingSink makeSink() {
                return new ReducingSink();
            }
        };
    }

    /**
     * Constructs a {@code TerminalOp} that implements a functional reduce on
     * reference values producing an optional reference result.
     *
     * @param <T> The type of the input elements, and the type of the result
     * @param operator The reducing function
     * @return A {@code TerminalOp} implementing the reduction
     */
    public static <T> TerminalOp<T, Optional<T>>
    makeRef(BinaryOperator<T> operator) {
        Objects.requireNonNull(operator);
        class ReducingSink
                implements AccumulatingSink<T, Optional<T>, ReducingSink> {
            private boolean empty;
            private T state;

            public void begin(long size) {
                empty = true;
                state = null;
            }

            @Override
            public void accept(T t) {
                if (empty) {
                    empty = false;
                    state = t;
                } else {
                    state = operator.apply(state, t);
                }
            }

            @Override
            public Optional<T> get() {
                return empty ? Optional.empty() : Optional.of(state);
            }

            @Override
            public void combine(ReducingSink other) {
                if (!other.empty)
                    accept(other.state);
            }
        }
        return new ReduceOp<T, Optional<T>, ReducingSink>(StreamShape.REFERENCE) {
            @Override
            public ReducingSink makeSink() {
                return new ReducingSink();
            }
        };
    }

    /**
     * Constructs a {@code TerminalOp} that implements a mutable reduce on
     * reference values.
     *
     * @param <T> the type of the input elements
     * @param <I> the type of the intermediate reduction result
     * @param collector a {@code Collector} defining the reduction
     * @return a {@code ReduceOp} implementing the reduction
     */
    // [Collector 버전 collect — Stream.collect(collector)]
    //
    // Collector의 네 함수를 ReducingSink에 직접 연결한다:
    //   supplier()    → begin()에서 컨테이너 생성
    //   accumulator() → accept()에서 원소를 컨테이너에 누적
    //   combiner()    → combine()에서 병렬 서브태스크 컨테이너 병합
    //
    // UNORDERED 특성이 있으면 NOT_ORDERED 플래그를 설정하여 순서 제약을 해제한다.
    // (Collector.finisher()는 ReferencePipeline.collect()에서 별도로 호출됨)
    public static <T, I> TerminalOp<T, I>
    makeRef(Collector<? super T, I, ?> collector) {
        Supplier<I> supplier = Objects.requireNonNull(collector).supplier();
        BiConsumer<I, ? super T> accumulator = collector.accumulator();
        BinaryOperator<I> combiner = collector.combiner();
        class ReducingSink extends Box<I>
                implements AccumulatingSink<T, I, ReducingSink> {
            @Override
            public void begin(long size) {
                state = supplier.get();   // 각 (서브)태스크마다 독립 컨테이너 생성
            }

            @Override
            public void accept(T t) {
                accumulator.accept(state, t);   // 컨테이너에 원소 누적 (가변 축약)
            }

            @Override
            public void combine(ReducingSink other) {
                // 병렬 완료 시 오른쪽 컨테이너를 왼쪽에 병합
                state = combiner.apply(state, other.state);
            }
        }
        return new ReduceOp<T, I, ReducingSink>(StreamShape.REFERENCE) {
            @Override
            public ReducingSink makeSink() {
                return new ReducingSink();
            }

            @Override
            public int getOpFlags() {
                // UNORDERED collector이면 순서 제약 해제 → 병렬 최적화 가능
                return collector.characteristics().contains(Collector.Characteristics.UNORDERED)
                       ? StreamOpFlag.NOT_ORDERED
                       : 0;
            }
        };
    }

    /**
     * Constructs a {@code TerminalOp} that implements a mutable reduce on
     * reference values.
     *
     * @param <T> the type of the input elements
     * @param <R> the type of the result
     * @param seedFactory a factory to produce a new base accumulator
     * @param accumulator a function to incorporate an element into an
     *        accumulator
     * @param reducer a function to combine an accumulator into another
     * @return a {@code TerminalOp} implementing the reduction
     */
    public static <T, R> TerminalOp<T, R>
    makeRef(Supplier<R> seedFactory,
            BiConsumer<R, ? super T> accumulator,
            BiConsumer<R,R> reducer) {
        Objects.requireNonNull(seedFactory);
        Objects.requireNonNull(accumulator);
        Objects.requireNonNull(reducer);
        class ReducingSink extends Box<R>
                implements AccumulatingSink<T, R, ReducingSink> {
            @Override
            public void begin(long size) {
                state = seedFactory.get();
            }

            @Override
            public void accept(T t) {
                accumulator.accept(state, t);
            }

            @Override
            public void combine(ReducingSink other) {
                reducer.accept(state, other.state);
            }
        }
        return new ReduceOp<T, R, ReducingSink>(StreamShape.REFERENCE) {
            @Override
            public ReducingSink makeSink() {
                return new ReducingSink();
            }
        };
    }

    /**
     * Constructs a {@code TerminalOp} that counts the number of stream
     * elements.  If the size of the pipeline is known then count is the size
     * and there is no need to evaluate the pipeline.  If the size of the
     * pipeline is non known then count is produced, via reduction, using a
     * {@link CountingSink}.
     *
     * @param <T> the type of the input elements
     * @return a {@code TerminalOp} implementing the counting
     */
    public static <T> TerminalOp<T, Long>
    makeRefCounting() {
        return new ReduceOp<T, Long, CountingSink<T>>(StreamShape.REFERENCE) {
            @Override
            public CountingSink<T> makeSink() { return new CountingSink.OfRef<>(); }

            @Override
            public <P_IN> Long evaluateSequential(PipelineHelper<T> helper,
                                                  Spliterator<P_IN> spliterator) {
                long size = helper.exactOutputSizeIfKnown(spliterator);
                if (size != -1)
                    return size;
                return super.evaluateSequential(helper, spliterator);
            }

            @Override
            public <P_IN> Long evaluateParallel(PipelineHelper<T> helper,
                                                Spliterator<P_IN> spliterator) {
                long size = helper.exactOutputSizeIfKnown(spliterator);
                if (size != -1)
                    return size;
                return super.evaluateParallel(helper, spliterator);
            }

            @Override
            public int getOpFlags() {
                return StreamOpFlag.NOT_ORDERED;
            }
        };
    }

    /**
     * Constructs a {@code TerminalOp} that implements a functional reduce on
     * {@code int} values.
     *
     * @param identity the identity for the combining function
     * @param operator the combining function
     * @return a {@code TerminalOp} implementing the reduction
     */
    public static TerminalOp<Integer, Integer>
    makeInt(int identity, IntBinaryOperator operator) {
        Objects.requireNonNull(operator);
        class ReducingSink
                implements AccumulatingSink<Integer, Integer, ReducingSink>, Sink.OfInt {
            private int state;

            @Override
            public void begin(long size) {
                state = identity;
            }

            @Override
            public void accept(int t) {
                state = operator.applyAsInt(state, t);
            }

            @Override
            public Integer get() {
                return state;
            }

            @Override
            public void combine(ReducingSink other) {
                accept(other.state);
            }
        }
        return new ReduceOp<Integer, Integer, ReducingSink>(StreamShape.INT_VALUE) {
            @Override
            public ReducingSink makeSink() {
                return new ReducingSink();
            }
        };
    }

    /**
     * Constructs a {@code TerminalOp} that implements a functional reduce on
     * {@code int} values, producing an optional integer result.
     *
     * @param operator the combining function
     * @return a {@code TerminalOp} implementing the reduction
     */
    public static TerminalOp<Integer, OptionalInt>
    makeInt(IntBinaryOperator operator) {
        Objects.requireNonNull(operator);
        class ReducingSink
                implements AccumulatingSink<Integer, OptionalInt, ReducingSink>, Sink.OfInt {
            private boolean empty;
            private int state;

            public void begin(long size) {
                empty = true;
                state = 0;
            }

            @Override
            public void accept(int t) {
                if (empty) {
                    empty = false;
                    state = t;
                }
                else {
                    state = operator.applyAsInt(state, t);
                }
            }

            @Override
            public OptionalInt get() {
                return empty ? OptionalInt.empty() : OptionalInt.of(state);
            }

            @Override
            public void combine(ReducingSink other) {
                if (!other.empty)
                    accept(other.state);
            }
        }
        return new ReduceOp<Integer, OptionalInt, ReducingSink>(StreamShape.INT_VALUE) {
            @Override
            public ReducingSink makeSink() {
                return new ReducingSink();
            }
        };
    }

    /**
     * Constructs a {@code TerminalOp} that implements a mutable reduce on
     * {@code int} values.
     *
     * @param <R> The type of the result
     * @param supplier a factory to produce a new accumulator of the result type
     * @param accumulator a function to incorporate an int into an
     *        accumulator
     * @param combiner a function to combine an accumulator into another
     * @return A {@code ReduceOp} implementing the reduction
     */
    public static <R> TerminalOp<Integer, R>
    makeInt(Supplier<R> supplier,
            ObjIntConsumer<R> accumulator,
            BinaryOperator<R> combiner) {
        Objects.requireNonNull(supplier);
        Objects.requireNonNull(accumulator);
        Objects.requireNonNull(combiner);
        class ReducingSink extends Box<R>
                implements AccumulatingSink<Integer, R, ReducingSink>, Sink.OfInt {
            @Override
            public void begin(long size) {
                state = supplier.get();
            }

            @Override
            public void accept(int t) {
                accumulator.accept(state, t);
            }

            @Override
            public void combine(ReducingSink other) {
                state = combiner.apply(state, other.state);
            }
        }
        return new ReduceOp<Integer, R, ReducingSink>(StreamShape.INT_VALUE) {
            @Override
            public ReducingSink makeSink() {
                return new ReducingSink();
            }
        };
    }

    /**
     * Constructs a {@code TerminalOp} that counts the number of stream
     * elements.  If the size of the pipeline is known then count is the size
     * and there is no need to evaluate the pipeline.  If the size of the
     * pipeline is non known then count is produced, via reduction, using a
     * {@link CountingSink}.
     *
     * @return a {@code TerminalOp} implementing the counting
     */
    public static TerminalOp<Integer, Long>
    makeIntCounting() {
        return new ReduceOp<Integer, Long, CountingSink<Integer>>(StreamShape.INT_VALUE) {
            @Override
            public CountingSink<Integer> makeSink() { return new CountingSink.OfInt(); }

            @Override
            public <P_IN> Long evaluateSequential(PipelineHelper<Integer> helper,
                                                  Spliterator<P_IN> spliterator) {
                long size = helper.exactOutputSizeIfKnown(spliterator);
                if (size != -1)
                    return size;
                return super.evaluateSequential(helper, spliterator);
            }

            @Override
            public <P_IN> Long evaluateParallel(PipelineHelper<Integer> helper,
                                                Spliterator<P_IN> spliterator) {
                long size = helper.exactOutputSizeIfKnown(spliterator);
                if (size != -1)
                    return size;
                return super.evaluateParallel(helper, spliterator);
            }

            @Override
            public int getOpFlags() {
                return StreamOpFlag.NOT_ORDERED;
            }
        };
    }

    /**
     * Constructs a {@code TerminalOp} that implements a functional reduce on
     * {@code long} values.
     *
     * @param identity the identity for the combining function
     * @param operator the combining function
     * @return a {@code TerminalOp} implementing the reduction
     */
    public static TerminalOp<Long, Long>
    makeLong(long identity, LongBinaryOperator operator) {
        Objects.requireNonNull(operator);
        class ReducingSink
                implements AccumulatingSink<Long, Long, ReducingSink>, Sink.OfLong {
            private long state;

            @Override
            public void begin(long size) {
                state = identity;
            }

            @Override
            public void accept(long t) {
                state = operator.applyAsLong(state, t);
            }

            @Override
            public Long get() {
                return state;
            }

            @Override
            public void combine(ReducingSink other) {
                accept(other.state);
            }
        }
        return new ReduceOp<Long, Long, ReducingSink>(StreamShape.LONG_VALUE) {
            @Override
            public ReducingSink makeSink() {
                return new ReducingSink();
            }
        };
    }

    /**
     * Constructs a {@code TerminalOp} that implements a functional reduce on
     * {@code long} values, producing an optional long result.
     *
     * @param operator the combining function
     * @return a {@code TerminalOp} implementing the reduction
     */
    public static TerminalOp<Long, OptionalLong>
    makeLong(LongBinaryOperator operator) {
        Objects.requireNonNull(operator);
        class ReducingSink
                implements AccumulatingSink<Long, OptionalLong, ReducingSink>, Sink.OfLong {
            private boolean empty;
            private long state;

            public void begin(long size) {
                empty = true;
                state = 0;
            }

            @Override
            public void accept(long t) {
                if (empty) {
                    empty = false;
                    state = t;
                }
                else {
                    state = operator.applyAsLong(state, t);
                }
            }

            @Override
            public OptionalLong get() {
                return empty ? OptionalLong.empty() : OptionalLong.of(state);
            }

            @Override
            public void combine(ReducingSink other) {
                if (!other.empty)
                    accept(other.state);
            }
        }
        return new ReduceOp<Long, OptionalLong, ReducingSink>(StreamShape.LONG_VALUE) {
            @Override
            public ReducingSink makeSink() {
                return new ReducingSink();
            }
        };
    }

    /**
     * Constructs a {@code TerminalOp} that implements a mutable reduce on
     * {@code long} values.
     *
     * @param <R> the type of the result
     * @param supplier a factory to produce a new accumulator of the result type
     * @param accumulator a function to incorporate an int into an
     *        accumulator
     * @param combiner a function to combine an accumulator into another
     * @return a {@code TerminalOp} implementing the reduction
     */
    public static <R> TerminalOp<Long, R>
    makeLong(Supplier<R> supplier,
             ObjLongConsumer<R> accumulator,
             BinaryOperator<R> combiner) {
        Objects.requireNonNull(supplier);
        Objects.requireNonNull(accumulator);
        Objects.requireNonNull(combiner);
        class ReducingSink extends Box<R>
                implements AccumulatingSink<Long, R, ReducingSink>, Sink.OfLong {
            @Override
            public void begin(long size) {
                state = supplier.get();
            }

            @Override
            public void accept(long t) {
                accumulator.accept(state, t);
            }

            @Override
            public void combine(ReducingSink other) {
                state = combiner.apply(state, other.state);
            }
        }
        return new ReduceOp<Long, R, ReducingSink>(StreamShape.LONG_VALUE) {
            @Override
            public ReducingSink makeSink() {
                return new ReducingSink();
            }
        };
    }

    /**
     * Constructs a {@code TerminalOp} that counts the number of stream
     * elements.  If the size of the pipeline is known then count is the size
     * and there is no need to evaluate the pipeline.  If the size of the
     * pipeline is non known then count is produced, via reduction, using a
     * {@link CountingSink}.
     *
     * @return a {@code TerminalOp} implementing the counting
     */
    public static TerminalOp<Long, Long>
    makeLongCounting() {
        return new ReduceOp<Long, Long, CountingSink<Long>>(StreamShape.LONG_VALUE) {
            @Override
            public CountingSink<Long> makeSink() { return new CountingSink.OfLong(); }

            @Override
            public <P_IN> Long evaluateSequential(PipelineHelper<Long> helper,
                                                  Spliterator<P_IN> spliterator) {
                long size = helper.exactOutputSizeIfKnown(spliterator);
                if (size != -1)
                    return size;
                return super.evaluateSequential(helper, spliterator);
            }

            @Override
            public <P_IN> Long evaluateParallel(PipelineHelper<Long> helper,
                                                Spliterator<P_IN> spliterator) {
                long size = helper.exactOutputSizeIfKnown(spliterator);
                if (size != -1)
                    return size;
                return super.evaluateParallel(helper, spliterator);
            }

            @Override
            public int getOpFlags() {
                return StreamOpFlag.NOT_ORDERED;
            }
        };
    }

    /**
     * Constructs a {@code TerminalOp} that implements a functional reduce on
     * {@code double} values.
     *
     * @param identity the identity for the combining function
     * @param operator the combining function
     * @return a {@code TerminalOp} implementing the reduction
     */
    public static TerminalOp<Double, Double>
    makeDouble(double identity, DoubleBinaryOperator operator) {
        Objects.requireNonNull(operator);
        class ReducingSink
                implements AccumulatingSink<Double, Double, ReducingSink>, Sink.OfDouble {
            private double state;

            @Override
            public void begin(long size) {
                state = identity;
            }

            @Override
            public void accept(double t) {
                state = operator.applyAsDouble(state, t);
            }

            @Override
            public Double get() {
                return state;
            }

            @Override
            public void combine(ReducingSink other) {
                accept(other.state);
            }
        }
        return new ReduceOp<Double, Double, ReducingSink>(StreamShape.DOUBLE_VALUE) {
            @Override
            public ReducingSink makeSink() {
                return new ReducingSink();
            }
        };
    }

    /**
     * Constructs a {@code TerminalOp} that implements a functional reduce on
     * {@code double} values, producing an optional double result.
     *
     * @param operator the combining function
     * @return a {@code TerminalOp} implementing the reduction
     */
    public static TerminalOp<Double, OptionalDouble>
    makeDouble(DoubleBinaryOperator operator) {
        Objects.requireNonNull(operator);
        class ReducingSink
                implements AccumulatingSink<Double, OptionalDouble, ReducingSink>, Sink.OfDouble {
            private boolean empty;
            private double state;

            public void begin(long size) {
                empty = true;
                state = 0;
            }

            @Override
            public void accept(double t) {
                if (empty) {
                    empty = false;
                    state = t;
                }
                else {
                    state = operator.applyAsDouble(state, t);
                }
            }

            @Override
            public OptionalDouble get() {
                return empty ? OptionalDouble.empty() : OptionalDouble.of(state);
            }

            @Override
            public void combine(ReducingSink other) {
                if (!other.empty)
                    accept(other.state);
            }
        }
        return new ReduceOp<Double, OptionalDouble, ReducingSink>(StreamShape.DOUBLE_VALUE) {
            @Override
            public ReducingSink makeSink() {
                return new ReducingSink();
            }
        };
    }

    /**
     * Constructs a {@code TerminalOp} that implements a mutable reduce on
     * {@code double} values.
     *
     * @param <R> the type of the result
     * @param supplier a factory to produce a new accumulator of the result type
     * @param accumulator a function to incorporate an int into an
     *        accumulator
     * @param combiner a function to combine an accumulator into another
     * @return a {@code TerminalOp} implementing the reduction
     */
    public static <R> TerminalOp<Double, R>
    makeDouble(Supplier<R> supplier,
               ObjDoubleConsumer<R> accumulator,
               BinaryOperator<R> combiner) {
        Objects.requireNonNull(supplier);
        Objects.requireNonNull(accumulator);
        Objects.requireNonNull(combiner);
        class ReducingSink extends Box<R>
                implements AccumulatingSink<Double, R, ReducingSink>, Sink.OfDouble {
            @Override
            public void begin(long size) {
                state = supplier.get();
            }

            @Override
            public void accept(double t) {
                accumulator.accept(state, t);
            }

            @Override
            public void combine(ReducingSink other) {
                state = combiner.apply(state, other.state);
            }
        }
        return new ReduceOp<Double, R, ReducingSink>(StreamShape.DOUBLE_VALUE) {
            @Override
            public ReducingSink makeSink() {
                return new ReducingSink();
            }
        };
    }

    /**
     * Constructs a {@code TerminalOp} that counts the number of stream
     * elements.  If the size of the pipeline is known then count is the size
     * and there is no need to evaluate the pipeline.  If the size of the
     * pipeline is non known then count is produced, via reduction, using a
     * {@link CountingSink}.
     *
     * @return a {@code TerminalOp} implementing the counting
     */
    public static TerminalOp<Double, Long>
    makeDoubleCounting() {
        return new ReduceOp<Double, Long, CountingSink<Double>>(StreamShape.DOUBLE_VALUE) {
            @Override
            public CountingSink<Double> makeSink() { return new CountingSink.OfDouble(); }

            @Override
            public <P_IN> Long evaluateSequential(PipelineHelper<Double> helper,
                                                  Spliterator<P_IN> spliterator) {
                long size = helper.exactOutputSizeIfKnown(spliterator);
                if (size != -1)
                    return size;
                return super.evaluateSequential(helper, spliterator);
            }

            @Override
            public <P_IN> Long evaluateParallel(PipelineHelper<Double> helper,
                                                Spliterator<P_IN> spliterator) {
                long size = helper.exactOutputSizeIfKnown(spliterator);
                if (size != -1)
                    return size;
                return super.evaluateParallel(helper, spliterator);
            }

            @Override
            public int getOpFlags() {
                return StreamOpFlag.NOT_ORDERED;
            }
        };
    }

    /**
     * A sink that counts elements
     */
    abstract static class CountingSink<T>
            extends Box<Long>
            implements AccumulatingSink<T, Long, CountingSink<T>> {
        long count;

        @Override
        public void begin(long size) {
            count = 0L;
        }

        @Override
        public Long get() {
            return count;
        }

        @Override
        public void combine(CountingSink<T> other) {
            count += other.count;
        }

        static final class OfRef<T> extends CountingSink<T> {
            @Override
            public void accept(T t) {
                count++;
            }
        }

        static final class OfInt extends CountingSink<Integer> implements Sink.OfInt {
            @Override
            public void accept(int t) {
                count++;
            }
        }

        static final class OfLong extends CountingSink<Long> implements Sink.OfLong {
            @Override
            public void accept(long t) {
                count++;
            }
        }

        static final class OfDouble extends CountingSink<Double> implements Sink.OfDouble {
            @Override
            public void accept(double t) {
                count++;
            }
        }
    }

    /**
     * A type of {@code TerminalSink} that implements an associative reducing
     * operation on elements of type {@code T} and producing a result of type
     * {@code R}.
     *
     * @param <T> the type of input element to the combining operation
     * @param <R> the result type
     * @param <K> the type of the {@code AccumulatingSink}.
     */
    private interface AccumulatingSink<T, R, K extends AccumulatingSink<T, R, K>>
            extends TerminalSink<T, R> {
        void combine(K other);
    }

    /**
     * State box for a single state element, used as a base class for
     * {@code AccumulatingSink} instances
     *
     * @param <U> The type of the state element
     */
    private abstract static class Box<U> {
        U state;

        Box() {} // Avoid creation of special accessor

        public U get() {
            return state;
        }
    }

    /**
     * A {@code TerminalOp} that evaluates a stream pipeline and sends the
     * output into an {@code AccumulatingSink}, which performs a reduce
     * operation. The {@code AccumulatingSink} must represent an associative
     * reducing operation.
     *
     * @param <T> the output type of the stream pipeline
     * @param <R> the result type of the reducing operation
     * @param <S> the type of the {@code AccumulatingSink}
     */
    // [TerminalOp 구현 — 축약 연산의 실행 진입점]
    //
    // evaluateSequential:
    //   helper.wrapAndCopyInto(makeSink(), spliterator).get()
    //   → 싱크 체인을 조립하고, 소스 원소를 밀어 넣은 후 결과를 꺼낸다.
    //
    // evaluateParallel:
    //   new ReduceTask(this, helper, spliterator).invoke().get()
    //   → ForkJoin 태스크로 분할 병렬 처리 후 결과 반환.
    private abstract static class ReduceOp<T, R, S extends AccumulatingSink<T, R, S>>
            implements TerminalOp<T, R> {
        private final StreamShape inputShape;

        /**
         * Create a {@code ReduceOp} of the specified stream shape which uses
         * the specified {@code Supplier} to create accumulating sinks.
         *
         * @param shape The shape of the stream pipeline
         */
        ReduceOp(StreamShape shape) {
            inputShape = shape;
        }

        public abstract S makeSink();

        @Override
        public StreamShape inputShape() {
            return inputShape;
        }

        @Override
        public <P_IN> R evaluateSequential(PipelineHelper<T> helper,
                                           Spliterator<P_IN> spliterator) {
            // 순차 실행: 싱크 체인 조립 → 소스 원소 push → 결과 반환
            return helper.wrapAndCopyInto(makeSink(), spliterator).get();
        }

        @Override
        public <P_IN> R evaluateParallel(PipelineHelper<T> helper,
                                         Spliterator<P_IN> spliterator) {
            // 병렬 실행: ForkJoin 태스크 생성 → invoke()로 완료를 기다림 → 결과 반환.
            // 실제 분할과 fork는 상위 AbstractTask.compute()에서 수행한다.
            return new ReduceTask<>(this, helper, spliterator).invoke().get();
        }
    }

    /**
     * A {@code ForkJoinTask} for performing a parallel reduce operation.
     */
    // [병렬 축약을 위한 ForkJoin 태스크]
    //
    // 분할-정복(divide and conquer) 패턴:
    //   1. AbstractTask.compute()가 spliterator.trySplit()으로 소스를 반으로 분할.
    //   2. 왼쪽/오른쪽 서브태스크를 재귀적으로 fork.
    //   3. 리프 태스크(더 이상 분할 불가)는 doLeaf()를 호출하여 순차적으로 처리.
    //   4. 각 서브태스크가 완료되면 onCompletion()에서 결과를 병합.
    //
    // doLeaf():       helper.wrapAndCopyInto(op.makeSink(), spliterator) — 싱크 체인으로 처리
    // onCompletion(): leftResult.combine(rightResult) — 두 부분 결과를 하나로 합침
    @SuppressWarnings("serial")
    private static final class ReduceTask<P_IN, P_OUT, R,
                                          S extends AccumulatingSink<P_OUT, R, S>>
            extends AbstractTask<P_IN, P_OUT, S, ReduceTask<P_IN, P_OUT, R, S>> {
        private final ReduceOp<P_OUT, R, S> op;

        ReduceTask(ReduceOp<P_OUT, R, S> op,
                   PipelineHelper<P_OUT> helper,
                   Spliterator<P_IN> spliterator) {
            super(helper, spliterator);
            this.op = op;
        }

        ReduceTask(ReduceTask<P_IN, P_OUT, R, S> parent,
                   Spliterator<P_IN> spliterator) {
            super(parent, spliterator);
            this.op = parent.op;
        }

        @Override
        protected ReduceTask<P_IN, P_OUT, R, S> makeChild(Spliterator<P_IN> spliterator) {
            return new ReduceTask<>(this, spliterator);
        }

        @Override
        protected S doLeaf() {
            // 리프 태스크: 이 파티션의 원소들을 순차적으로 싱크 체인으로 처리
            return helper.wrapAndCopyInto(op.makeSink(), spliterator);
        }

        @Override
        public void onCompletion(CountedCompleter<?> caller) {
            if (!isLeaf()) {
                // 내부 노드: 왼쪽 서브태스크 결과에 오른쪽 결과를 combine()으로 병합
                // 결합 순서가 올바른 결과를 위해 왼쪽 기준으로 병합한다
                S leftResult = leftChild.getLocalResult();
                leftResult.combine(rightChild.getLocalResult());
                setLocalResult(leftResult);
            }
            // GC spliterator, left and right child
            super.onCompletion(caller);
        }
    }
}
