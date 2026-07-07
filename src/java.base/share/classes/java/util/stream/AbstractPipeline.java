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

import java.util.Objects;
import java.util.Spliterator;
import java.util.function.IntFunction;
import java.util.function.Supplier;

/**
 * Abstract base class for "pipeline" classes, which are the core
 * implementations of the Stream interface and its primitive specializations.
 * Manages construction and evaluation of stream pipelines.
 *
 * <p>An {@code AbstractPipeline} represents an initial portion of a stream
 * pipeline, encapsulating a stream source and zero or more intermediate
 * operations.  The individual {@code AbstractPipeline} objects are often
 * referred to as <em>stages</em>, where each stage describes either the stream
 * source or an intermediate operation.
 *
 * <p>A concrete intermediate stage is generally built from an
 * {@code AbstractPipeline}, a shape-specific pipeline class which extends it
 * (e.g., {@code IntPipeline}) which is also abstract, and an operation-specific
 * concrete class which extends that.  {@code AbstractPipeline} contains most of
 * the mechanics of evaluating the pipeline, and implements methods that will be
 * used by the operation; the shape-specific classes add helper methods for
 * dealing with collection of results into the appropriate shape-specific
 * containers.
 *
 * <p>After chaining a new intermediate operation, or executing a terminal
 * operation, the stream is considered to be consumed, and no more intermediate
 * or terminal operations are permitted on this stream instance.
 *
 * @implNote
 * <p>For sequential streams, and parallel streams without
 * <a href="package-summary.html#StreamOps">stateful intermediate
 * operations</a>, parallel streams, pipeline evaluation is done in a single
 * pass that "jams" all the operations together.  For parallel streams with
 * stateful operations, execution is divided into segments, where each
 * stateful operations marks the end of a segment, and each segment is
 * evaluated separately and the result used as the input to the next
 * segment.  In all cases, the source data is not consumed until a terminal
 * operation begins.
 *
 * @param <E_IN>  type of input elements
 * @param <E_OUT> type of output elements
 * @param <S> type of the subclass implementing {@code BaseStream}
 * @since 1.8
 */
// [스트림 파이프라인의 핵심 뼈대]
// Stream 파이프라인의 각 "스테이지(stage)"를 나타내는 추상 기반 클래스.
// .filter(), .map() 등 중간 연산을 호출할 때마다 이 클래스의 서브클래스 인스턴스가
// 하나씩 생성되어 양방향 연결 리스트(doubly-linked list)로 이어진다.
// 데이터는 터미널 연산(forEach, collect 등)이 호출되기 전까지 전혀 흐르지 않는다 — 게으른 평가(lazy evaluation).
abstract class AbstractPipeline<E_IN, E_OUT, S extends BaseStream<E_OUT, S>>
        extends PipelineHelper<E_OUT> implements BaseStream<E_OUT, S> {
    private static final String MSG_STREAM_LINKED = "stream has already been operated upon or closed";
    private static final String MSG_CONSUMED = "source already consumed or closed";

    /**
     * Backlink to the head of the pipeline chain (self if this is the source
     * stage).
     */
    // 파이프라인 체인의 헤드(소스 스테이지)를 가리키는 역방향 포인터.
    // 소스 스테이지 자신이면 this를 가리킨다.
    // sourceSpliterator, parallel 플래그 등 소스 관련 상태에 접근할 때 사용된다.
    @SuppressWarnings("rawtypes")
    private final AbstractPipeline sourceStage;

    /**
     * The "upstream" pipeline, or null if this is the source stage.
     */
    // 이전(upstream) 스테이지 포인터. 소스 스테이지이면 null.
    // wrapSink()에서 싱크 체인을 뒤에서 앞으로 감쌀 때 이 포인터를 따라 역방향으로 순회한다.
    @SuppressWarnings("rawtypes")
    protected final AbstractPipeline previousStage;

    /**
     * The operation flags for the intermediate operation represented by this
     * pipeline object.
     */
    // 이 스테이지 자체의 연산 플래그 (예: NOT_SIZED, NOT_SORTED, IS_SHORT_CIRCUIT 등).
    // StreamOpFlag 비트마스크로 표현되며, 소스이면 STREAM_MASK, 중간 연산이면 OP_MASK로 마스킹된다.
    protected final int sourceOrOpFlags;

    /**
     * The next stage in the pipeline, or null if this is the last stage.
     * Effectively final at the point of linking to the next pipeline.
     */
    // 다음(downstream) 스테이지 포인터. 마지막 스테이지이면 null.
    // 중간 연산 생성자에서 previousStage.nextStage = this 로 세팅된다.
    @SuppressWarnings("rawtypes")
    private AbstractPipeline nextStage;

    /**
     * The number of intermediate operations between this pipeline object
     * and the stream source if sequential, or the previous stateful if parallel.
     * Valid at the point of pipeline preparation for evaluation.
     */
    // 소스로부터 이 스테이지까지의 깊이(거리). 소스는 0.
    // 병렬 스트림에서 stateful 연산이 나타나면 depth가 0으로 리셋된다 — 해당 지점부터 새 세그먼트가 시작됨.
    private int depth;

    /**
     * The combined source and operation flags for the source and all operations
     * up to and including the operation represented by this pipeline object.
     * Valid at the point of pipeline preparation for evaluation.
     */
    // 소스부터 이 스테이지까지 모든 플래그의 누적 합성값.
    // 새 스테이지가 추가될 때마다 StreamOpFlag.combineOpFlags()로 갱신된다.
    // copyInto()에서 SHORT_CIRCUIT 여부를 확인하는 등 최적화 판단에 사용된다.
    private int combinedFlags;

    /**
     * The source spliterator. Only valid for the head pipeline.
     * Before the pipeline is consumed if non-null then {@code sourceSupplier}
     * must be null. After the pipeline is consumed if non-null then is set to
     * null.
     */
    // 데이터 소스를 탐색하는 Spliterator. 헤드 스테이지에만 존재한다.
    // Collection.stream()처럼 이미 구체화된 소스일 때 여기에 저장된다.
    // 터미널 연산이 시작되면 sourceSpliterator(terminalFlags)에서 꺼내 쓰고 null로 소거된다.
    private Spliterator<?> sourceSpliterator;

    /**
     * The source supplier. Only valid for the head pipeline. Before the
     * pipeline is consumed if non-null then {@code sourceSpliterator} must be
     * null. After the pipeline is consumed if non-null then is set to null.
     */
    // Spliterator를 지연(lazy) 생성하는 공급자. 헤드 스테이지에만 존재한다.
    // StreamSupport.stream(supplier, ...)처럼 평가 시점까지 소스 생성을 미룰 때 사용된다.
    // 터미널 연산 시점에 .get()이 호출되어 실제 Spliterator가 만들어진다.
    private Supplier<? extends Spliterator<?>> sourceSupplier;

    /**
     * True if this pipeline has been linked or consumed
     */
    // 이 스테이지가 이미 다운스트림에 연결되었거나 터미널 연산으로 소비되었음을 나타내는 플래그.
    // true가 되면 이 스트림에 추가적인 중간/터미널 연산을 체이닝할 수 없다 — 스트림의 단일 사용 보장.
    private boolean linkedOrConsumed;

    private Runnable sourceCloseAction;

    /**
     * True if pipeline is parallel, otherwise the pipeline is sequential; only
     * valid for the source stage.
     */
    private boolean parallel;

    /**
     * Constructor for the head of a stream pipeline.
     *
     * @param source {@code Supplier<Spliterator>} describing the stream source
     * @param sourceFlags The source flags for the stream source, described in
     * {@link StreamOpFlag}
     * @param parallel True if the pipeline is parallel
     */
    // [소스 스테이지 생성자 — 지연(lazy) Spliterator 공급자 버전]
    // StreamSupport.stream(Supplier<Spliterator>, ...) 경로에서 사용된다.
    // 터미널 연산이 실행될 때까지 실제 Spliterator 생성을 미룬다.
    AbstractPipeline(Supplier<? extends Spliterator<?>> source,
                     int sourceFlags, boolean parallel) {
        this.previousStage = null;
        this.sourceSupplier = source;
        this.sourceStage = this;   // 자기 자신이 헤드
        this.sourceOrOpFlags = sourceFlags & StreamOpFlag.STREAM_MASK;
        // The following is an optimization of:
        // StreamOpFlag.combineOpFlags(sourceOrOpFlags, StreamOpFlag.INITIAL_OPS_VALUE);
        this.combinedFlags = (~(sourceOrOpFlags << 1)) & StreamOpFlag.INITIAL_OPS_VALUE;
        this.depth = 0;
        this.parallel = parallel;
    }

    /**
     * Constructor for the head of a stream pipeline.
     *
     * @param source {@code Spliterator} describing the stream source
     * @param sourceFlags the source flags for the stream source, described in
     * {@link StreamOpFlag}
     * @param parallel {@code true} if the pipeline is parallel
     */
    // [소스 스테이지 생성자 — 즉시(eager) Spliterator 버전]
    // Collection.stream(), Arrays.stream() 등에서 사용된다.
    // Spliterator가 이미 준비된 상태로 전달된다.
    AbstractPipeline(Spliterator<?> source,
                     int sourceFlags, boolean parallel) {
        this.previousStage = null;
        this.sourceSpliterator = source;
        this.sourceStage = this;   // 자기 자신이 헤드
        this.sourceOrOpFlags = sourceFlags & StreamOpFlag.STREAM_MASK;
        // The following is an optimization of:
        // StreamOpFlag.combineOpFlags(sourceOrOpFlags, StreamOpFlag.INITIAL_OPS_VALUE);
        this.combinedFlags = (~(sourceOrOpFlags << 1)) & StreamOpFlag.INITIAL_OPS_VALUE;
        this.depth = 0;
        this.parallel = parallel;
    }

    /**
     * Constructor for appending an intermediate operation stage onto an
     * existing pipeline.
     *
     * The previous stage must be unlinked and unconsumed.
     *
     * @param previousStage the upstream pipeline stage
     * @param opFlags the operation flags for the new stage, described in
     * {@link StreamOpFlag}
     * @throws IllegalStateException if previousStage is already linked or
     * consumed
     */
    // [중간 연산 스테이지 생성자 — 파이프라인 체인 확장]
    // .filter(), .map() 등 모든 중간 연산이 호출될 때마다 이 생성자가 실행된다.
    //
    // 핵심 동작:
    // 1. previousStage.linkedOrConsumed = true  → 이전 스테이지는 이제 "소비됨", 재사용 불가
    // 2. previousStage.nextStage = this          → 순방향 포인터 연결
    // 3. this.previousStage = previousStage      → 역방향 포인터 연결
    // 4. combinedFlags를 누적 합성                → 파이프라인 전체 속성 추적
    // 5. depth++                                  → 소스로부터의 거리 증가
    //
    // 이 시점에 데이터는 전혀 처리되지 않는다. 오직 노드 구조만 구축된다.
    AbstractPipeline(AbstractPipeline<?, E_IN, ?> previousStage, int opFlags) {
        if (previousStage.linkedOrConsumed)
            throw new IllegalStateException(MSG_STREAM_LINKED);
        previousStage.linkedOrConsumed = true;
        previousStage.nextStage = this;   // 순방향 포인터 세팅

        this.previousStage = previousStage;   // 역방향 포인터 세팅
        this.sourceOrOpFlags = opFlags & StreamOpFlag.OP_MASK;
        // 이전 스테이지까지의 누적 플래그에 이 연산의 플래그를 합성
        this.combinedFlags = StreamOpFlag.combineOpFlags(opFlags, previousStage.combinedFlags);
        this.sourceStage = previousStage.sourceStage;   // 헤드 포인터를 그대로 전달
        this.depth = previousStage.depth + 1;
    }

    /**
     * Constructor for replacing an intermediate operation stage onto an
     * existing pipeline.
     *
     * @param previousPreviousStage the upstream pipeline stage of the upstream pipeline stage
     * @param previousStage the upstream pipeline stage
     * @param opFlags the operation flags for the new stage, described in
     * {@link StreamOpFlag}
     * @throws IllegalStateException if previousStage is already linked or
     * consumed
     */
    protected AbstractPipeline(AbstractPipeline<?, E_IN, ?> previousPreviousStage, AbstractPipeline<?, E_IN, ?> previousStage, int opFlags) {
        if (previousStage.linkedOrConsumed || !previousPreviousStage.linkedOrConsumed || previousPreviousStage.nextStage != previousStage || previousStage.previousStage != previousPreviousStage)
            throw new IllegalStateException(MSG_STREAM_LINKED);

        previousStage.linkedOrConsumed = true;

        previousPreviousStage.nextStage = this;

        this.previousStage = previousPreviousStage;
        this.sourceOrOpFlags = opFlags & StreamOpFlag.OP_MASK;
        this.combinedFlags = StreamOpFlag.combineOpFlags(opFlags, previousPreviousStage.combinedFlags);
        this.sourceStage = previousPreviousStage.sourceStage;
        this.depth = previousPreviousStage.depth + 1;
    }

    /**
     * Checks that the current stage has not been already linked or consumed,
     * and then sets this stage as being linked or consumed.
     */
    protected void linkOrConsume() {
        if (linkedOrConsumed)
            throw new IllegalStateException(MSG_STREAM_LINKED);
        linkedOrConsumed = true;
    }

    // Terminal evaluation methods

    /**
     * Evaluate the pipeline with a terminal operation to produce a result.
     *
     * @param <R> the type of result
     * @param terminalOp the terminal operation to be applied to the pipeline.
     * @return the result
     */
    // [실행 진입점 — 터미널 연산이 파이프라인을 평가할 때 반드시 이 메서드를 거친다]
    //
    // 흐름:
    // 1. linkedOrConsumed = true 로 소비 마킹 (두 번 호출 방지)
    // 2. sourceSpliterator()로 소스를 확보 (지연된 Supplier가 있다면 이 시점에 실체화됨)
    // 3. 순차(sequential)이면 terminalOp.evaluateSequential() 호출
    //    병렬(parallel)이면 terminalOp.evaluateParallel() → ForkJoin 태스크로 분기
    final <R> R evaluate(TerminalOp<E_OUT, R> terminalOp) {
        assert getOutputShape() == terminalOp.inputShape();
        if (linkedOrConsumed)
            throw new IllegalStateException(MSG_STREAM_LINKED);
        linkedOrConsumed = true;

        return isParallel()
               ? terminalOp.evaluateParallel(this, sourceSpliterator(terminalOp.getOpFlags()))
               : terminalOp.evaluateSequential(this, sourceSpliterator(terminalOp.getOpFlags()));
    }

    /**
     * Collect the elements output from the pipeline stage.
     *
     * @param generator the array generator to be used to create array instances
     * @return a flat array-backed Node that holds the collected output elements
     */
    @SuppressWarnings("unchecked")
    final Node<E_OUT> evaluateToArrayNode(IntFunction<E_OUT[]> generator) {
        if (linkedOrConsumed)
            throw new IllegalStateException(MSG_STREAM_LINKED);
        linkedOrConsumed = true;

        // If the last intermediate operation is stateful then
        // evaluate directly to avoid an extra collection step
        if (isParallel() && previousStage != null && opIsStateful()) {
            // Set the depth of this, last, pipeline stage to zero to slice the
            // pipeline such that this operation will not be included in the
            // upstream slice and upstream operations will not be included
            // in this slice
            depth = 0;
            return opEvaluateParallel(previousStage, previousStage.sourceSpliterator(0), generator);
        }
        else {
            return evaluate(sourceSpliterator(0), true, generator);
        }
    }

    /**
     * Gets the source stage spliterator if this pipeline stage is the source
     * stage.  The pipeline is consumed after this method is called and
     * returns successfully.
     *
     * @return the source stage spliterator
     * @throws IllegalStateException if this pipeline stage is not the source
     *         stage.
     */

    final Spliterator<E_OUT> sourceStageSpliterator() {
        // Ensures that this method is only ever called on the sourceStage
        if (this != sourceStage)
            throw new IllegalStateException();

        if (linkedOrConsumed)
            throw new IllegalStateException(MSG_STREAM_LINKED);
        linkedOrConsumed = true;

        if (sourceSpliterator != null) {
            @SuppressWarnings("unchecked")
            Spliterator<E_OUT> s = (Spliterator<E_OUT>)sourceSpliterator;
            sourceSpliterator = null;
            return s;
        }
        else if (sourceSupplier != null) {
            @SuppressWarnings("unchecked")
            Spliterator<E_OUT> s = (Spliterator<E_OUT>)sourceSupplier.get();
            sourceSupplier = null;
            return s;
        }
        else {
            throw new IllegalStateException(MSG_CONSUMED);
        }
    }

    // BaseStream

    @Override
    @SuppressWarnings("unchecked")
    public final S sequential() {
        sourceStage.parallel = false;
        return (S) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public final S parallel() {
        sourceStage.parallel = true;
        return (S) this;
    }

    @Override
    public void close() {
        linkedOrConsumed = true;
        sourceSupplier = null;
        sourceSpliterator = null;
        Runnable closeAction = sourceStage.sourceCloseAction;
        if (closeAction != null) {
            sourceStage.sourceCloseAction = null;
            closeAction.run();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public S onClose(Runnable closeHandler) {
        if (linkedOrConsumed)
            throw new IllegalStateException(MSG_STREAM_LINKED);
        Objects.requireNonNull(closeHandler);
        Runnable existingHandler = sourceStage.sourceCloseAction;
        sourceStage.sourceCloseAction =
                (existingHandler == null)
                ? closeHandler
                : Streams.composeWithExceptions(existingHandler, closeHandler);
        return (S) this;
    }

    // Primitive specialization use co-variant overrides, hence is not final
    @Override
    @SuppressWarnings("unchecked")
    public Spliterator<E_OUT> spliterator() {
        if (linkedOrConsumed)
            throw new IllegalStateException(MSG_STREAM_LINKED);
        linkedOrConsumed = true;

        if (this == sourceStage) {
            if (sourceStage.sourceSpliterator != null) {
                @SuppressWarnings("unchecked")
                Spliterator<E_OUT> s = (Spliterator<E_OUT>) sourceStage.sourceSpliterator;
                sourceStage.sourceSpliterator = null;
                return s;
            }
            else if (sourceStage.sourceSupplier != null) {
                @SuppressWarnings("unchecked")
                Supplier<Spliterator<E_OUT>> s = (Supplier<Spliterator<E_OUT>>) sourceStage.sourceSupplier;
                sourceStage.sourceSupplier = null;
                return lazySpliterator(s);
            }
            else {
                throw new IllegalStateException(MSG_CONSUMED);
            }
        }
        else {
            return wrap(this, () -> sourceSpliterator(0), isParallel());
        }
    }

    @Override
    public final boolean isParallel() {
        return sourceStage.parallel;
    }


    /**
     * Returns the composition of stream flags of the stream source and all
     * intermediate operations.
     *
     * @return the composition of stream flags of the stream source and all
     *         intermediate operations
     * @see StreamOpFlag
     */
    final int getStreamFlags() {
        return StreamOpFlag.toStreamFlags(combinedFlags);
    }

    /**
     * Returns whether any of the stages of the current segment is stateful
     * or not.
     * @return {@code true} if any stage in this segment is stateful,
     *         {@code false} if not.
     */
    protected final boolean hasAnyStateful() {
         var result = false;
         for (var u = sourceStage.nextStage;
              u != null && !(result = u.opIsStateful()) && u != this;
              u = u.nextStage) {
         }
         return result;
     }

    /**
     * Returns whether any of the stages in the (entire) pipeline is short-circuiting
     * or not.
     * @return {@code true} if any stage in this pipeline is short-circuiting,
     *         {@code false} if not.
     */
    protected final boolean isShortCircuitingPipeline() {
        for (var u = sourceStage.nextStage; u != null; u = u.nextStage) {
            if (StreamOpFlag.SHORT_CIRCUIT.isKnown(u.combinedFlags))
                return true;
        }
        return false;
    }

    /**
     * Get the source spliterator for this pipeline stage.  For a sequential or
     * stateless parallel pipeline, this is the source spliterator.  For a
     * stateful parallel pipeline, this is a spliterator describing the results
     * of all computations up to and including the most recent stateful
     * operation.
     */
    @SuppressWarnings("unchecked")
    protected Spliterator<?> sourceSpliterator(int terminalFlags) {
        // Get the source spliterator of the pipeline
        Spliterator<?> spliterator = null;
        if (sourceStage.sourceSpliterator != null) {
            spliterator = sourceStage.sourceSpliterator;
            sourceStage.sourceSpliterator = null;
        }
        else if (sourceStage.sourceSupplier != null) {
            spliterator = (Spliterator<?>) sourceStage.sourceSupplier.get();
            sourceStage.sourceSupplier = null;
        }
        else {
            throw new IllegalStateException(MSG_CONSUMED);
        }

        if (isParallel() && hasAnyStateful()) {
            // Adapt the source spliterator, evaluating each stateful op
            // in the pipeline up to and including this pipeline stage.
            // The depth and flags of each pipeline stage are adjusted accordingly.
            int depth = 1;
            for (@SuppressWarnings("rawtypes") AbstractPipeline u = sourceStage, p = sourceStage.nextStage, e = this;
                 u != e;
                 u = p, p = p.nextStage) {

                int thisOpFlags = p.sourceOrOpFlags;
                if (p.opIsStateful()) {
                    depth = 0;

                    if (StreamOpFlag.SHORT_CIRCUIT.isKnown(thisOpFlags)) {
                        // Clear the short circuit flag for next pipeline stage
                        // This stage encapsulates short-circuiting, the next
                        // stage may not have any short-circuit operations, and
                        // if so spliterator.forEachRemaining should be used
                        // for traversal
                        thisOpFlags = thisOpFlags & ~StreamOpFlag.IS_SHORT_CIRCUIT;
                    }

                    spliterator = p.opEvaluateParallelLazy(u, spliterator);

                    // Inject or clear SIZED on the source pipeline stage
                    // based on the stage's spliterator
                    thisOpFlags = spliterator.hasCharacteristics(Spliterator.SIZED)
                            ? (thisOpFlags & ~StreamOpFlag.NOT_SIZED) | StreamOpFlag.IS_SIZED
                            : (thisOpFlags & ~StreamOpFlag.IS_SIZED) | StreamOpFlag.NOT_SIZED;
                }
                p.depth = depth++;
                p.combinedFlags = StreamOpFlag.combineOpFlags(thisOpFlags, u.combinedFlags);
            }
        }

        if (terminalFlags != 0)  {
            // Apply flags from the terminal operation to last pipeline stage
            combinedFlags = StreamOpFlag.combineOpFlags(terminalFlags, combinedFlags);
        }

        return spliterator;
    }

    // PipelineHelper

    @Override
    final StreamShape getSourceShape() {
        @SuppressWarnings("rawtypes")
        AbstractPipeline p = AbstractPipeline.this;
        while (p.depth > 0) {
            p = p.previousStage;
        }
        return p.getOutputShape();
    }

    @Override
    final <P_IN> long exactOutputSizeIfKnown(Spliterator<P_IN> spliterator) {
        int flags = getStreamAndOpFlags();
        long size = StreamOpFlag.SIZED.isKnown(flags) ? spliterator.getExactSizeIfKnown() : -1;
        // Currently, we have no stateless SIZE_ADJUSTING intermediate operations,
        // so we can simply ignore SIZE_ADJUSTING in parallel streams, since adjustments
        // are already accounted in the input spliterator.
        //
        // If we ever have a stateless SIZE_ADJUSTING intermediate operation,
        // we would need step back until depth == 0, then call exactOutputSize() for
        // the subsequent stages.
        if (size != -1 && StreamOpFlag.SIZE_ADJUSTING.isKnown(flags) && !isParallel()) {
            // Skip the source stage as it's never SIZE_ADJUSTING
            for (AbstractPipeline<?, ?, ?> stage = sourceStage.nextStage; stage != null; stage = stage.nextStage) {
                size = stage.exactOutputSize(size);
            }
        }
        return size;
    }

    /**
     * Returns the exact output size of the pipeline given the exact size reported by the previous stage.
     *
     * @param previousSize the exact size reported by the previous stage
     * @return the output size of this stage
     */
    long exactOutputSize(long previousSize) {
        return previousSize;
    }

    // [싱크 체인 조립 + 데이터 밀어넣기를 한 번에 수행하는 편의 메서드]
    // wrapSink(sink) 로 싱크 체인을 만든 다음, 바로 copyInto()로 데이터를 밀어 넣는다.
    // TerminalOp의 evaluateSequential() 구현에서 주로 사용된다.
    // 반환값은 wrapSink에 전달된 최종(터미널) Sink 자신이므로, 호출자가 .get()으로 결과를 꺼낼 수 있다.
    @Override
    final <P_IN, S extends Sink<E_OUT>> S wrapAndCopyInto(S sink, Spliterator<P_IN> spliterator) {
        copyInto(wrapSink(Objects.requireNonNull(sink)), spliterator);
        return sink;
    }

    // [데이터 푸시 엔진 — 실제로 원소를 Spliterator에서 꺼내 싱크 체인으로 밀어 넣는다]
    //
    // 두 가지 경로:
    // A. 일반 경로 (SHORT_CIRCUIT 플래그 없음):
    //    begin() → spliterator.forEachRemaining(sink) → end()
    //    모든 원소를 한꺼번에 밀어 넣는 가장 빠른 경로.
    //
    // B. 단락(short-circuit) 경로 (findFirst, limit, anyMatch 등):
    //    copyIntoWithCancel()로 위임 → 각 원소마다 sink.cancellationRequested() 체크.
    //    조건 충족 시 즉시 중단.
    @Override
    final <P_IN> void copyInto(Sink<P_IN> wrappedSink, Spliterator<P_IN> spliterator) {
        Objects.requireNonNull(wrappedSink);

        if (!StreamOpFlag.SHORT_CIRCUIT.isKnown(getStreamAndOpFlags())) {
            // 일반 경로: 모든 원소를 한 번에 처리
            wrappedSink.begin(spliterator.getExactSizeIfKnown());
            spliterator.forEachRemaining(wrappedSink);
            wrappedSink.end();
        }
        else {
            // 단락 경로: 원소마다 취소 요청 여부를 확인하며 처리
            copyIntoWithCancel(wrappedSink, spliterator);
        }
    }

    // [단락 경로 — 원소마다 cancellationRequested()를 확인하며 push]
    // forEachWithCancel()은 각 tryAdvance() 이후 sink.cancellationRequested()를 체크한다.
    // findFirst/findAny가 값을 찾거나 limit이 한도에 달하면 true를 반환하여 루프를 탈출시킨다.
    @Override
    @SuppressWarnings("unchecked")
    final <P_IN> boolean copyIntoWithCancel(Sink<P_IN> wrappedSink, Spliterator<P_IN> spliterator) {
        @SuppressWarnings("rawtypes")
        AbstractPipeline p = AbstractPipeline.this;
        while (p.depth > 0) {
            p = p.previousStage;
        }

        wrappedSink.begin(spliterator.getExactSizeIfKnown());
        boolean cancelled = p.forEachWithCancel(spliterator, wrappedSink);
        wrappedSink.end();
        return cancelled;
    }

    @Override
    final int getStreamAndOpFlags() {
        return combinedFlags;
    }

    final boolean isOrdered() {
        return StreamOpFlag.ORDERED.isKnown(combinedFlags);
    }

    // [싱크 체인 역방향 조립 — 파이프라인의 핵심 알고리즘]
    //
    // 터미널 싱크(sink)를 시작점으로, 현재 스테이지에서 소스 방향으로 역순 순회하며
    // 각 스테이지의 opWrapSink()를 호출해 싱크를 겹겹이 감싼다.
    //
    // 예) filter → map → reduce 파이프라인의 경우:
    //   wrapSink(reduceSink)
    //   → p=mapStage:    sink = mapStage.opWrapSink(flags, reduceSink)  → MapSink(reduceSink)
    //   → p=filterStage: sink = filterStage.opWrapSink(flags, MapSink)  → FilterSink(MapSink)
    //   반환값: FilterSink (소스에서 가장 먼저 원소를 받는 입구 싱크)
    //
    // 실행 시에는 FilterSink.accept(e) → MapSink.accept(e) → ReduceSink.accept(e) 순으로
    // 원소가 앞쪽에서 뒤쪽으로 흘러간다.
    @Override
    @SuppressWarnings("unchecked")
    final <P_IN> Sink<P_IN> wrapSink(Sink<E_OUT> sink) {
        Objects.requireNonNull(sink);

        // depth > 0 인 동안 역방향으로 각 스테이지의 싱크를 감싸며 체인 구성
        for ( @SuppressWarnings("rawtypes") AbstractPipeline p=AbstractPipeline.this; p.depth > 0; p=p.previousStage) {
            sink = p.opWrapSink(p.previousStage.combinedFlags, sink);
        }
        return (Sink<P_IN>) sink;
    }

    @Override
    @SuppressWarnings("unchecked")
    final <P_IN> Spliterator<E_OUT> wrapSpliterator(Spliterator<P_IN> sourceSpliterator) {
        if (depth == 0) {
            return (Spliterator<E_OUT>) sourceSpliterator;
        }
        else {
            return wrap(this, () -> sourceSpliterator, isParallel());
        }
    }

    @Override
    final <P_IN> Node<E_OUT> evaluate(Spliterator<P_IN> spliterator,
                                      boolean flatten,
                                      IntFunction<E_OUT[]> generator) {
        if (isParallel()) {
            // @@@ Optimize if op of this pipeline stage is a stateful op
            return evaluateToNode(this, spliterator, flatten, generator);
        }
        else {
            Node.Builder<E_OUT> nb = makeNodeBuilder(
                    exactOutputSizeIfKnown(spliterator), generator);
            return wrapAndCopyInto(nb, spliterator).build();
        }
    }


    // Shape-specific abstract methods, implemented by XxxPipeline classes

    /**
     * Get the output shape of the pipeline.  If the pipeline is the head,
     * then its output shape corresponds to the shape of the source.
     * Otherwise, its output shape corresponds to the output shape of the
     * associated operation.
     *
     * @return the output shape
     */
    abstract StreamShape getOutputShape();

    /**
     * Collect elements output from a pipeline into a Node that holds elements
     * of this shape.
     *
     * @param helper the pipeline helper describing the pipeline stages
     * @param spliterator the source spliterator
     * @param flattenTree true if the returned node should be flattened
     * @param generator the array generator
     * @return a Node holding the output of the pipeline
     */
    abstract <P_IN> Node<E_OUT> evaluateToNode(PipelineHelper<E_OUT> helper,
                                               Spliterator<P_IN> spliterator,
                                               boolean flattenTree,
                                               IntFunction<E_OUT[]> generator);

    /**
     * Create a spliterator that wraps a source spliterator, compatible with
     * this stream shape, and operations associated with a {@link
     * PipelineHelper}.
     *
     * @param ph the pipeline helper describing the pipeline stages
     * @param supplier the supplier of a spliterator
     * @return a wrapping spliterator compatible with this shape
     */
    abstract <P_IN> Spliterator<E_OUT> wrap(PipelineHelper<E_OUT> ph,
                                            Supplier<Spliterator<P_IN>> supplier,
                                            boolean isParallel);

    /**
     * Create a lazy spliterator that wraps and obtains the supplied the
     * spliterator when a method is invoked on the lazy spliterator.
     * @param supplier the supplier of a spliterator
     */
    abstract Spliterator<E_OUT> lazySpliterator(Supplier<? extends Spliterator<E_OUT>> supplier);

    /**
     * Traverse the elements of a spliterator compatible with this stream shape,
     * pushing those elements into a sink.   If the sink requests cancellation,
     * no further elements will be pulled or pushed.
     *
     * @param spliterator the spliterator to pull elements from
     * @param sink the sink to push elements to
     * @return true if the cancellation was requested
     */
    abstract boolean forEachWithCancel(Spliterator<E_OUT> spliterator, Sink<E_OUT> sink);

    /**
     * Make a node builder compatible with this stream shape.
     *
     * @param exactSizeIfKnown if {@literal >=0}, then a node builder will be
     * created that has a fixed capacity of at most sizeIfKnown elements. If
     * {@literal < 0}, then the node builder has an unfixed capacity. A fixed
     * capacity node builder will throw exceptions if an element is added after
     * builder has reached capacity, or is built before the builder has reached
     * capacity.
     *
     * @param generator the array generator to be used to create instances of a
     * T[] array. For implementations supporting primitive nodes, this parameter
     * may be ignored.
     * @return a node builder
     */
    @Override
    abstract Node.Builder<E_OUT> makeNodeBuilder(long exactSizeIfKnown,
                                                 IntFunction<E_OUT[]> generator);


    // Op-specific abstract methods, implemented by the operation class

    /**
     * Returns whether this operation is stateful or not.  If it is stateful,
     * then the method
     * {@link #opEvaluateParallel(PipelineHelper, java.util.Spliterator, java.util.function.IntFunction)}
     * must be overridden.
     *
     * @return {@code true} if this operation is stateful
     */
    abstract boolean opIsStateful();

    /**
     * Accepts a {@code Sink} which will receive the results of this operation,
     * and return a {@code Sink} which accepts elements of the input type of
     * this operation and which performs the operation, passing the results to
     * the provided {@code Sink}.
     *
     * @apiNote
     * The implementation may use the {@code flags} parameter to optimize the
     * sink wrapping.  For example, if the input is already {@code DISTINCT},
     * the implementation for the {@code Stream#distinct()} method could just
     * return the sink it was passed.
     *
     * @param flags The combined stream and operation flags up to, but not
     *        including, this operation
     * @param sink sink to which elements should be sent after processing
     * @return a sink which accepts elements, perform the operation upon
     *         each element, and passes the results (if any) to the provided
     *         {@code Sink}.
     */
    abstract Sink<E_IN> opWrapSink(int flags, Sink<E_OUT> sink);

    /**
     * Performs a parallel evaluation of the operation using the specified
     * {@code PipelineHelper} which describes the upstream intermediate
     * operations.  Only called on stateful operations.  If {@link
     * #opIsStateful()} returns true then implementations must override the
     * default implementation.
     *
     * @implSpec The default implementation always throw
     * {@code UnsupportedOperationException}.
     *
     * @param helper the pipeline helper describing the pipeline stages
     * @param spliterator the source {@code Spliterator}
     * @param generator the array generator
     * @return a {@code Node} describing the result of the evaluation
     */
    <P_IN> Node<E_OUT> opEvaluateParallel(PipelineHelper<E_OUT> helper,
                                          Spliterator<P_IN> spliterator,
                                          IntFunction<E_OUT[]> generator) {
        throw new UnsupportedOperationException("Parallel evaluation is not supported");
    }

    /**
     * Returns a {@code Spliterator} describing a parallel evaluation of the
     * operation, using the specified {@code PipelineHelper} which describes the
     * upstream intermediate operations.  Only called on stateful operations.
     * It is not necessary (though acceptable) to do a full computation of the
     * result here; it is preferable, if possible, to describe the result via a
     * lazily evaluated spliterator.
     *
     * @implSpec The default implementation behaves as if:
     * <pre>{@code
     *     return evaluateParallel(helper, i -> (E_OUT[]) new
     * Object[i]).spliterator();
     * }</pre>
     * and is suitable for implementations that cannot do better than a full
     * synchronous evaluation.
     *
     * @param helper the pipeline helper
     * @param spliterator the source {@code Spliterator}
     * @return a {@code Spliterator} describing the result of the evaluation
     */
    <P_IN> Spliterator<E_OUT> opEvaluateParallelLazy(PipelineHelper<E_OUT> helper,
                                                     Spliterator<P_IN> spliterator) {
        return opEvaluateParallel(helper, spliterator, Nodes.castingArray()).spliterator();
    }
}
