/*
 * Copyright (c) 2012, 2013, Oracle and/or its affiliates. All rights reserved.
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
import java.util.Spliterator;
import java.util.function.DoublePredicate;
import java.util.function.IntPredicate;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Factory for instances of a short-circuiting {@code TerminalOp} that implement
 * quantified predicate matching on the elements of a stream. Supported variants
 * include match-all, match-any, and match-none.
 *
 * @since 1.8
 */
// [단락(short-circuit) 조건 검사 터미널 연산 — anyMatch / allMatch / noneMatch]
//
// MatchKind 열거형으로 세 가지 의미론을 하나의 구현에 통합:
//
//   ANY:  (stopOnPredicateMatches=true,  shortCircuitResult=true)
//         predicate가 true인 원소를 만나면 즉시 true 반환.
//
//   ALL:  (stopOnPredicateMatches=false, shortCircuitResult=false)
//         predicate가 false인 원소를 만나면 즉시 false 반환.
//         (ALL의 경우 "predicate가 매치 안되면" 중단)
//
//   NONE: (stopOnPredicateMatches=true,  shortCircuitResult=false)
//         predicate가 true인 원소를 만나면 즉시 false 반환.
//
// 단락 메커니즘:
//   MatchSink.accept()에서 stop=true를 설정하면
//   BooleanTerminalSink.cancellationRequested()가 stop을 반환 → 루프 즉시 중단.
//
// 초기값: BooleanTerminalSink 생성자에서 value = !shortCircuitResult 으로 초기화.
//   → 원소가 없을 때 anyMatch=false, allMatch=true, noneMatch=true 를 자연스럽게 표현.
final class MatchOps {

    private MatchOps() { }

    /**
     * Enum describing quantified match options -- all match, any match, none
     * match.
     */
    // [매치 종류를 나타내는 열거형]
    //
    // stopOnPredicateMatches: predicate 결과가 이 값일 때 즉시 중단(단락)한다.
    //   anyMatch: predicate=true  → 매치됨 → 중단
    //   allMatch: predicate=false → 매치 안됨 → 중단
    //   noneMatch: predicate=true → 매치됨 → 중단
    //
    // shortCircuitResult: 단락 시 반환할 결과값.
    //   anyMatch: true (찾았으니 true)
    //   allMatch: false (반례를 찾았으니 false)
    //   noneMatch: false (매치를 찾았으니 false)
    enum MatchKind {
        /** Do any elements match the predicate? */
        ANY(true, true),

        /** Do all elements match the predicate? */
        ALL(false, false),

        /** Do no elements match the predicate? */
        NONE(true, false);

        private final boolean stopOnPredicateMatches;
        private final boolean shortCircuitResult;

        private MatchKind(boolean stopOnPredicateMatches,
                          boolean shortCircuitResult) {
            this.stopOnPredicateMatches = stopOnPredicateMatches;
            this.shortCircuitResult = shortCircuitResult;
        }
    }

    /**
     * Constructs a quantified predicate matcher for a Stream.
     *
     * @param <T> the type of stream elements
     * @param predicate the {@code Predicate} to apply to stream elements
     * @param matchKind the kind of quantified match (all, any, none)
     * @return a {@code TerminalOp} implementing the desired quantified match
     *         criteria
     */
    // [참조 타입 스트림용 매치 싱크 — anyMatch/allMatch/noneMatch의 실제 로직]
    //
    // accept()의 핵심 로직:
    //   predicate.test(t) == matchKind.stopOnPredicateMatches  이면 단락 조건 충족.
    //   stop = true, value = matchKind.shortCircuitResult 를 설정한다.
    //
    // 예) anyMatch(p):
    //   p.test(t) == true (ANY.stopOnPredicateMatches)  → stop=true, value=true
    //
    // 예) allMatch(p):
    //   !p.test(t) == false (ALL.stopOnPredicateMatches=false, !test == true일 때)
    //   즉 p.test(t)=false → stop=true, value=false
    //
    // BooleanTerminalSink.cancellationRequested()는 stop 필드를 반환한다.
    public static <T> TerminalOp<T, Boolean> makeRef(Predicate<? super T> predicate,
            MatchKind matchKind) {
        Objects.requireNonNull(predicate);
        Objects.requireNonNull(matchKind);
        class MatchSink extends BooleanTerminalSink<T> {
            MatchSink() {
                super(matchKind);
            }

            @Override
            public void accept(T t) {
                if (!stop && predicate.test(t) == matchKind.stopOnPredicateMatches) {
                    // 단락 조건 충족: 결과값 저장 후 stop=true로 루프 중단 신호
                    stop = true;
                    value = matchKind.shortCircuitResult;
                }
            }
        }

        return new MatchOp<>(StreamShape.REFERENCE, matchKind, MatchSink::new);
    }

    /**
     * Constructs a quantified predicate matcher for an {@code IntStream}.
     *
     * @param predicate the {@code Predicate} to apply to stream elements
     * @param matchKind the kind of quantified match (all, any, none)
     * @return a {@code TerminalOp} implementing the desired quantified match
     *         criteria
     */
    public static TerminalOp<Integer, Boolean> makeInt(IntPredicate predicate,
                                                       MatchKind matchKind) {
        Objects.requireNonNull(predicate);
        Objects.requireNonNull(matchKind);
        class MatchSink extends BooleanTerminalSink<Integer> implements Sink.OfInt {
            MatchSink() {
                super(matchKind);
            }

            @Override
            public void accept(int t) {
                if (!stop && predicate.test(t) == matchKind.stopOnPredicateMatches) {
                    stop = true;
                    value = matchKind.shortCircuitResult;
                }
            }
        }

        return new MatchOp<>(StreamShape.INT_VALUE, matchKind, MatchSink::new);
    }

    /**
     * Constructs a quantified predicate matcher for a {@code LongStream}.
     *
     * @param predicate the {@code Predicate} to apply to stream elements
     * @param matchKind the kind of quantified match (all, any, none)
     * @return a {@code TerminalOp} implementing the desired quantified match
     *         criteria
     */
    public static TerminalOp<Long, Boolean> makeLong(LongPredicate predicate,
                                                     MatchKind matchKind) {
        Objects.requireNonNull(predicate);
        Objects.requireNonNull(matchKind);
        class MatchSink extends BooleanTerminalSink<Long> implements Sink.OfLong {

            MatchSink() {
                super(matchKind);
            }

            @Override
            public void accept(long t) {
                if (!stop && predicate.test(t) == matchKind.stopOnPredicateMatches) {
                    stop = true;
                    value = matchKind.shortCircuitResult;
                }
            }
        }

        return new MatchOp<>(StreamShape.LONG_VALUE, matchKind, MatchSink::new);
    }

    /**
     * Constructs a quantified predicate matcher for a {@code DoubleStream}.
     *
     * @param predicate the {@code Predicate} to apply to stream elements
     * @param matchKind the kind of quantified match (all, any, none)
     * @return a {@code TerminalOp} implementing the desired quantified match
     *         criteria
     */
    public static TerminalOp<Double, Boolean> makeDouble(DoublePredicate predicate,
                                                         MatchKind matchKind) {
        Objects.requireNonNull(predicate);
        Objects.requireNonNull(matchKind);
        class MatchSink extends BooleanTerminalSink<Double> implements Sink.OfDouble {

            MatchSink() {
                super(matchKind);
            }

            @Override
            public void accept(double t) {
                if (!stop && predicate.test(t) == matchKind.stopOnPredicateMatches) {
                    stop = true;
                    value = matchKind.shortCircuitResult;
                }
            }
        }

        return new MatchOp<>(StreamShape.DOUBLE_VALUE, matchKind, MatchSink::new);
    }

    /**
     * A short-circuiting {@code TerminalOp} that evaluates a predicate on the
     * elements of a stream and determines whether all, any or none of those
     * elements match the predicate.
     *
     * @param <T> the output type of the stream pipeline
     */
    private static final class MatchOp<T> implements TerminalOp<T, Boolean> {
        private final StreamShape inputShape;
        final MatchKind matchKind;
        final Supplier<BooleanTerminalSink<T>> sinkSupplier;

        /**
         * Constructs a {@code MatchOp}.
         *
         * @param shape the output shape of the stream pipeline
         * @param matchKind the kind of quantified match (all, any, none)
         * @param sinkSupplier {@code Supplier} for a {@code Sink} of the
         *        appropriate shape which implements the matching operation
         */
        MatchOp(StreamShape shape,
                MatchKind matchKind,
                Supplier<BooleanTerminalSink<T>> sinkSupplier) {
            this.inputShape = shape;
            this.matchKind = matchKind;
            this.sinkSupplier = sinkSupplier;
        }

        @Override
        public int getOpFlags() {
            return StreamOpFlag.IS_SHORT_CIRCUIT | StreamOpFlag.NOT_ORDERED;
        }

        @Override
        public StreamShape inputShape() {
            return inputShape;
        }

        @Override
        public <S> Boolean evaluateSequential(PipelineHelper<T> helper,
                                              Spliterator<S> spliterator) {
            return helper.wrapAndCopyInto(sinkSupplier.get(), spliterator).getAndClearState();
        }

        @Override
        public <S> Boolean evaluateParallel(PipelineHelper<T> helper,
                                            Spliterator<S> spliterator) {
            // Approach for parallel implementation:
            // - Decompose as per usual
            // - run match on leaf chunks, call result "b"
            // - if b == matchKind.shortCircuitOn, complete early and return b
            // - else if we complete normally, return !shortCircuitOn

            return new MatchTask<>(this, helper, spliterator).invoke();
        }
    }

    /**
     * Boolean specific terminal sink to avoid the boxing costs when returning
     * results.  Subclasses implement the shape-specific functionality.
     *
     * @param <T> The output type of the stream pipeline
     */
    // [단락 매치 싱크의 기반 클래스]
    //
    // stop:  단락 신호 플래그. accept()에서 단락 조건이 충족되면 true로 설정.
    // value: 결과값. 초기값은 !shortCircuitResult.
    //   → 빈 스트림의 경우 anyMatch=false, allMatch=true, noneMatch=true 가 자연스럽게 나온다.
    //
    // cancellationRequested(): stop을 반환하여 단락 루프에 중단 신호 전달.
    // getAndClearState():      값을 반환 (boolean이므로 boxing 없음).
    private abstract static class BooleanTerminalSink<T> implements Sink<T> {
        boolean stop;
        boolean value;

        BooleanTerminalSink(MatchKind matchKind) {
            // 초기값: 단락 없이 모든 원소를 처리했을 때의 기본 결과
            // anyMatch → false (하나도 안 찾음), allMatch → true (모두 통과), noneMatch → true (하나도 없음)
            value = !matchKind.shortCircuitResult;
        }

        public boolean getAndClearState() {
            return value;
        }

        @Override
        public boolean cancellationRequested() {
            return stop;   // stop=true이면 더 이상 원소를 받지 않겠다는 신호
        }
    }

    /**
     * ForkJoinTask implementation to implement a parallel short-circuiting
     * quantified match
     *
     * @param <P_IN> the type of source elements for the pipeline
     * @param <P_OUT> the type of output elements for the pipeline
     */
    @SuppressWarnings("serial")
    private static final class MatchTask<P_IN, P_OUT>
            extends AbstractShortCircuitTask<P_IN, P_OUT, Boolean, MatchTask<P_IN, P_OUT>> {
        private final MatchOp<P_OUT> op;

        /**
         * Constructor for root node
         */
        MatchTask(MatchOp<P_OUT> op, PipelineHelper<P_OUT> helper,
                  Spliterator<P_IN> spliterator) {
            super(helper, spliterator);
            this.op = op;
        }

        /**
         * Constructor for non-root node
         */
        MatchTask(MatchTask<P_IN, P_OUT> parent, Spliterator<P_IN> spliterator) {
            super(parent, spliterator);
            this.op = parent.op;
        }

        @Override
        protected MatchTask<P_IN, P_OUT> makeChild(Spliterator<P_IN> spliterator) {
            return new MatchTask<>(this, spliterator);
        }

        @Override
        protected Boolean doLeaf() {
            boolean b = helper.wrapAndCopyInto(op.sinkSupplier.get(), spliterator).getAndClearState();
            if (b == op.matchKind.shortCircuitResult)
                shortCircuit(b);
            return null;
        }

        @Override
        protected Boolean getEmptyResult() {
            return !op.matchKind.shortCircuitResult;
        }
    }
}

