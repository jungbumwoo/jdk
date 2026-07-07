/*
 * Copyright (c) 2012, 2024, Oracle and/or its affiliates. All rights reserved.
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

import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Spliterator;
import java.util.concurrent.CountedCompleter;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Factory for instances of a short-circuiting {@code TerminalOp} that searches
 * for an element in a stream pipeline, and terminates when it finds one.
 * Supported variants include find-first (find the first element in the
 * encounter order) and find-any (find any element, may not be the first in
 * encounter order.)
 *
 * @since 1.8
 */
// [단락(short-circuit) 탐색 터미널 연산 — findFirst / findAny]
//
// findFirst: 소스 순서 기준 첫 번째 원소 반환 (IS_SHORT_CIRCUIT | 순서 유지)
// findAny:   어떤 원소든 찾는 즉시 반환 (IS_SHORT_CIRCUIT | NOT_ORDERED — 병렬에서 빠름)
//
// 단락 메커니즘:
//   FindSink.accept()가 첫 번째 원소를 받으면 hasValue = true 설정.
//   FindSink.cancellationRequested()가 hasValue를 반환 → true가 되는 순간
//   copyIntoWithCancel()의 루프가 더 이상 원소를 push하지 않는다.
//
// 병렬 실행:
//   FindTask(AbstractShortCircuitTask)가 서브태스크로 분할.
//   findAny: 가장 먼저 완료된 서브태스크가 shortCircuit(result)을 호출하여 나머지를 취소.
//   findFirst: 가장 왼쪽(leftmost) 서브태스크의 결과를 선택하여 순서를 보장.
final class FindOps {

    private FindOps() { }

    /**
     * Constructs a {@code TerminalOp} for streams of objects.
     *
     * @param <T> the type of elements of the stream
     * @param mustFindFirst whether the {@code TerminalOp} must produce the
     *        first element in the encounter order
     * @return a {@code TerminalOp} implementing the find operation
     */
    @SuppressWarnings("unchecked")
    public static <T> TerminalOp<T, Optional<T>> makeRef(boolean mustFindFirst) {
        return (TerminalOp<T, Optional<T>>)
                (mustFindFirst ? FindSink.OfRef.OP_FIND_FIRST : FindSink.OfRef.OP_FIND_ANY);
    }

    /**
     * Constructs a {@code TerminalOp} for streams of ints.
     *
     * @param mustFindFirst whether the {@code TerminalOp} must produce the
     *        first element in the encounter order
     * @return a {@code TerminalOp} implementing the find operation
     */
    public static TerminalOp<Integer, OptionalInt> makeInt(boolean mustFindFirst) {
        return mustFindFirst ? FindSink.OfInt.OP_FIND_FIRST : FindSink.OfInt.OP_FIND_ANY;
    }

    /**
     * Constructs a {@code TerminalOp} for streams of longs.
     *
     * @param mustFindFirst whether the {@code TerminalOp} must produce the
     *        first element in the encounter order
     * @return a {@code TerminalOp} implementing the find operation
     */
    public static TerminalOp<Long, OptionalLong> makeLong(boolean mustFindFirst) {
        return mustFindFirst ? FindSink.OfLong.OP_FIND_FIRST : FindSink.OfLong.OP_FIND_ANY;
    }

    /**
     * Constructs a {@code FindOp} for streams of doubles.
     *
     * @param mustFindFirst whether the {@code TerminalOp} must produce the
     *        first element in the encounter order
     * @return a {@code TerminalOp} implementing the find operation
     */
    public static TerminalOp<Double, OptionalDouble> makeDouble(boolean mustFindFirst) {
        return mustFindFirst ? FindSink.OfDouble.OP_FIND_FIRST : FindSink.OfDouble.OP_FIND_ANY;
    }

    /**
     * A short-circuiting {@code TerminalOp} that searches for an element in a
     * stream pipeline, and terminates when it finds one.  Implements both
     * find-first (find the first element in the encounter order) and find-any
     * (find any element, may not be the first in encounter order.)
     *
     * @param <T> the output type of the stream pipeline
     * @param <O> the result type of the find operation, typically an optional
     *        type
     */
    // [단락 탐색 TerminalOp 구현체]
    //
    // opFlags에 IS_SHORT_CIRCUIT을 항상 포함 → copyInto()가 단락 경로를 선택.
    // mustFindFirst=false(findAny)이면 추가로 NOT_ORDERED → 병렬 시 순서 무시 허용.
    //
    // evaluateSequential: wrapAndCopyInto(sinkSupplier.get(), spliterator).get()
    //   → FindSink가 첫 원소를 받으면 cancellationRequested()=true, 루프 중단.
    //
    // evaluateParallel: new FindTask(mustFindFirst, helper, spliterator).invoke()
    //   → 분할 탐색. findAny는 첫 완료 서브태스크가 shortCircuit()으로 전체 취소.
    //   → findFirst는 leftmost 서브태스크 결과를 선택(순서 보장).
    private static final class FindOp<T, O> implements TerminalOp<T, O> {
        private final StreamShape shape;
        final int opFlags;
        final O emptyValue;
        final Predicate<O> presentPredicate;
        final Supplier<TerminalSink<T, O>> sinkSupplier;

        /**
         * Constructs a {@code FindOp}.
         *
         * @param mustFindFirst if true, must find the first element in
         *        encounter order, otherwise can find any element
         * @param shape stream shape of elements to search
         * @param emptyValue result value corresponding to "found nothing"
         * @param presentPredicate {@code Predicate} on result value
         *        corresponding to "found something"
         * @param sinkSupplier supplier for a {@code TerminalSink} implementing
         *        the matching functionality
         */
        FindOp(boolean mustFindFirst,
                       StreamShape shape,
                       O emptyValue,
                       Predicate<O> presentPredicate,
                       Supplier<TerminalSink<T, O>> sinkSupplier) {
            // IS_SHORT_CIRCUIT: 항상 단락 연산임을 파이프라인에 알림
            // NOT_ORDERED(findAny만): 병렬 시 순서 무관하게 아무 원소나 반환 허용
            this.opFlags = StreamOpFlag.IS_SHORT_CIRCUIT | (mustFindFirst ? 0 : StreamOpFlag.NOT_ORDERED);
            this.shape = shape;
            this.emptyValue = emptyValue;
            this.presentPredicate = presentPredicate;
            this.sinkSupplier = sinkSupplier;
        }

        @Override
        public int getOpFlags() {
            return opFlags;
        }

        @Override
        public StreamShape inputShape() {
            return shape;
        }

        @Override
        public <S> O evaluateSequential(PipelineHelper<T> helper,
                                        Spliterator<S> spliterator) {
            O result = helper.wrapAndCopyInto(sinkSupplier.get(), spliterator).get();
            return result != null ? result : emptyValue;
        }

        @Override
        public <P_IN> O evaluateParallel(PipelineHelper<T> helper,
                                         Spliterator<P_IN> spliterator) {
            // This takes into account the upstream ops flags and the terminal
            // op flags and therefore takes into account findFirst or findAny
            boolean mustFindFirst = StreamOpFlag.ORDERED.isKnown(helper.getStreamAndOpFlags());
            return new FindTask<>(this, mustFindFirst, helper, spliterator).invoke();
        }
    }

    /**
     * Implementation of {@code TerminalSink} that implements the find
     * functionality, requesting cancellation when something has been found
     *
     * @param <T> The type of input element
     * @param <O> The result type, typically an optional type
     */
    // [단락 탐색 싱크 — 원소를 받자마자 중단 신호를 보내는 핵심 싱크]
    //
    // accept(): 첫 번째 원소만 저장하고 hasValue = true 설정. 이후 원소는 무시.
    // cancellationRequested(): hasValue를 그대로 반환.
    //   → 첫 원소를 받은 즉시 true가 되어 copyIntoWithCancel 루프를 탈출시킨다.
    //
    // OfRef.OP_FIND_FIRST / OP_FIND_ANY: 정적으로 미리 생성된 FindOp 싱글톤.
    //   동일한 FindOp 인스턴스를 재사용하지만, 매 평가마다 makeSink()로 새 FindSink를 생성한다.
    private abstract static class FindSink<T, O> implements TerminalSink<T, O> {
        boolean hasValue;   // 원소를 이미 찾았는지 여부
        T value;            // 찾은 원소

        FindSink() {} // Avoid creation of special accessor

        @Override
        public void accept(T value) {
            if (!hasValue) {
                hasValue = true;   // 첫 원소만 저장
                this.value = value;
            }
            // 이후 원소는 무시 — cancellationRequested()가 이미 true이므로 도달하지 않아야 함
        }

        @Override
        public boolean cancellationRequested() {
            // 원소를 찾은 순간 true → 소스에서 더 이상 원소를 push하지 않도록 신호
            return hasValue;
        }

        /** Specialization of {@code FindSink} for reference streams */
        static final class OfRef<T> extends FindSink<T, Optional<T>> {
            @Override
            public Optional<T> get() {
                return hasValue ? Optional.of(value) : null;
            }

            static final TerminalOp<?, ?> OP_FIND_FIRST, OP_FIND_ANY;
            static {
                Predicate<Optional<Object>> isPresent = Optional::isPresent;
                Supplier<TerminalSink<Object, Optional<Object>>> newSink
                        = FindSink.OfRef::new;
                OP_FIND_FIRST = new FindOp<>(true, StreamShape.REFERENCE,
                        Optional.empty(), isPresent, newSink);
                OP_FIND_ANY = new FindOp<>(false, StreamShape.REFERENCE,
                        Optional.empty(), isPresent, newSink);
            }
        }

        /** Specialization of {@code FindSink} for int streams */
        static final class OfInt extends FindSink<Integer, OptionalInt>
                implements Sink.OfInt {
            @Override
            public void accept(int value) {
                // Boxing is OK here, since few values will actually flow into the sink
                accept((Integer) value);
            }

            @Override
            public OptionalInt get() {
                return hasValue ? OptionalInt.of(value) : null;
            }

            static final TerminalOp<Integer, OptionalInt> OP_FIND_FIRST, OP_FIND_ANY;
            static {
                Predicate<OptionalInt> isPresent = OptionalInt::isPresent;
                Supplier<TerminalSink<Integer, OptionalInt>> newSink
                        = FindSink.OfInt::new;
                OP_FIND_FIRST = new FindOp<>(true, StreamShape.INT_VALUE,
                        OptionalInt.empty(), isPresent, newSink);
                OP_FIND_ANY = new FindOp<>(false, StreamShape.INT_VALUE,
                        OptionalInt.empty(), isPresent, newSink);
            }
        }

        /** Specialization of {@code FindSink} for long streams */
        static final class OfLong extends FindSink<Long, OptionalLong>
                implements Sink.OfLong {
            @Override
            public void accept(long value) {
                // Boxing is OK here, since few values will actually flow into the sink
                accept((Long) value);
            }

            @Override
            public OptionalLong get() {
                return hasValue ? OptionalLong.of(value) : null;
            }

            static final TerminalOp<Long, OptionalLong> OP_FIND_FIRST, OP_FIND_ANY;
            static {
                Predicate<OptionalLong> isPresent = OptionalLong::isPresent;
                Supplier<TerminalSink<Long, OptionalLong>> newSink
                        = FindSink.OfLong::new;
                OP_FIND_FIRST = new FindOp<>(true, StreamShape.LONG_VALUE,
                        OptionalLong.empty(), isPresent, newSink);
                OP_FIND_ANY = new FindOp<>(false, StreamShape.LONG_VALUE,
                        OptionalLong.empty(), isPresent, newSink);
            }
        }

        /** Specialization of {@code FindSink} for double streams */
        static final class OfDouble extends FindSink<Double, OptionalDouble>
                implements Sink.OfDouble {
            @Override
            public void accept(double value) {
                // Boxing is OK here, since few values will actually flow into the sink
                accept((Double) value);
            }

            @Override
            public OptionalDouble get() {
                return hasValue ? OptionalDouble.of(value) : null;
            }

            static final TerminalOp<Double, OptionalDouble> OP_FIND_FIRST, OP_FIND_ANY;
            static {
                Predicate<OptionalDouble> isPresent = OptionalDouble::isPresent;
                Supplier<TerminalSink<Double, OptionalDouble>> newSink
                        = FindSink.OfDouble::new;
                OP_FIND_FIRST = new FindOp<>(true, StreamShape.DOUBLE_VALUE,
                        OptionalDouble.empty(), isPresent, newSink);
                OP_FIND_ANY = new FindOp<>(false, StreamShape.DOUBLE_VALUE,
                        OptionalDouble.empty(), isPresent, newSink);
            }
        }
    }

    /**
     * {@code ForkJoinTask} implementing parallel short-circuiting search
     * @param <P_IN> Input element type to the stream pipeline
     * @param <P_OUT> Output element type from the stream pipeline
     * @param <O> Result type from the find operation
     */
    @SuppressWarnings("serial")
    private static final class FindTask<P_IN, P_OUT, O>
            extends AbstractShortCircuitTask<P_IN, P_OUT, O, FindTask<P_IN, P_OUT, O>> {
        private final FindOp<P_OUT, O> op;
        private final boolean mustFindFirst;

        FindTask(FindOp<P_OUT, O> op,
                 boolean mustFindFirst,
                 PipelineHelper<P_OUT> helper,
                 Spliterator<P_IN> spliterator) {
            super(helper, spliterator);
            this.mustFindFirst = mustFindFirst;
            this.op = op;
        }

        FindTask(FindTask<P_IN, P_OUT, O> parent, Spliterator<P_IN> spliterator) {
            super(parent, spliterator);
            this.mustFindFirst = parent.mustFindFirst;
            this.op = parent.op;
        }

        @Override
        protected FindTask<P_IN, P_OUT, O> makeChild(Spliterator<P_IN> spliterator) {
            return new FindTask<>(this, spliterator);
        }

        @Override
        protected O getEmptyResult() {
            return op.emptyValue;
        }

        private void foundResult(O answer) {
            if (isLeftmostNode())
                shortCircuit(answer);
            else
                cancelLaterNodes();
        }

        @Override
        protected O doLeaf() {
            O result = helper.wrapAndCopyInto(op.sinkSupplier.get(), spliterator).get();
            if (!mustFindFirst) {
                if (result != null)
                    shortCircuit(result);
                return null;
            }
            else {
                if (result != null) {
                    foundResult(result);
                    return result;
                }
                else
                    return null;
            }
        }

        @Override
        public void onCompletion(CountedCompleter<?> caller) {
            if (mustFindFirst) {
                    for (FindTask<P_IN, P_OUT, O> child = leftChild, p = null; child != p;
                         p = child, child = rightChild) {
                    O result = child.getLocalResult();
                    if (result != null && op.presentPredicate.test(result)) {
                        setLocalResult(result);
                        foundResult(result);
                        break;
                    }
                }
            }
            super.onCompletion(caller);
        }
    }
}

