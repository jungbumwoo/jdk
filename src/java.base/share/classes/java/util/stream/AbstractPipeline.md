Q.
src/java.base/share/classes/java/util/stream/AbstractPipeline.java 에 stage 개념에 대해서 설명해주세요. src/java.base/share/classes/java/util/stream/AbstractPipeline.java, src/java.base/share/classes/java/util/stream/ReferencePipeline.java, ReferencePipelin.StatefulOp, StatelessOp이 어떤 관계이고 어떤 방식으
로 stream이 동작하는지 설명해주세요. 도식화나 시각적으로 설명해줄 수 있으면 해당 부분을 추가해서 설명해주세요.

A.
1. Stage란 무엇인가

Stream에서 stage는 다음 중 하나를 나타내는 AbstractPipeline 객체입니다.

- 소스 stage: Collection, 배열, Spliterator 등
- 중간 연산 stage: filter, map, sorted, distinct 등
- 터미널 연산은 stage가 아니라 별도의 TerminalOp 객체

Stream<String> stream = list.stream()
.filter(s -> !s.isEmpty())
.map(String::toUpperCase);

이 코드는 데이터를 즉시 처리하지 않고 다음 객체 구조만 만듭니다.

ReferencePipeline.Head
source: list.spliterator()
│
▼
ReferencePipeline.StatelessOp       filter stage
│
▼
ReferencePipeline.StatelessOp       map stage

중간 연산을 호출할 때마다 새 stage가 생성되고, 그 새 stage가 새로운 Stream 객체로 반환됩니다. 연결 작업은 src/java.base/share/classes/java/util/stream/AbstractPipeline.java:243에서 수행 (AbstractPipeline(AbstractPipeline<?, E_IN, ?> previousStage, int opFlags) {)

previousStage ◀──── currentStage
│                  │
└── nextStage ────▶┘

각 stage:
sourceStage   ────────▶ Head
previousStage ────────▶ 이전 연산
nextStage     ────────▶ 다음 연산
depth                  소스 또는 이전 병렬 segment와의 거리
sourceOrOpFlags        이 stage 자체의 특성
combinedFlags          여기까지 누적된 특성

이때 이전 stage의 linkedOrConsumed가 true가 되므로 같은 Stream 객체에서 두 갈래 파이프라인을 만들 수 없습니다.

———

2. 클래스 관계

                     PipelineHelper<E_OUT>
                              ▲
                              │
BaseStream<E_OUT,S> ◀── AbstractPipeline<E_IN,E_OUT,S>
                                ▲
                                │ 객체 타입 Stream에 특화
                    ReferencePipeline<P_IN,P_OUT>
                    ▲             ▲             ▲
                    │             │             │
                    Head       StatelessOp     StatefulOp
                    source      filter/map      sorted/distinct
                    peek/flatMap     limit/skip

역할을 나누면 다음과 같습니다.

- src/java.base/share/classes/java/util/stream/AbstractPipeline.java:77: stage 연결, flags, 지연 평가, source 확보, Sink 체인 조립 등 공통 실행 엔진
- src/java.base/share/classes/java/util/stream/ReferencePipeline.java:72: 객체 참조형 Stream<T>에 필요한 API와 REFERENCE shape 처리
- src/java.base/share/classes/java/util/stream/ReferencePipeline.java:827: 소스를 표현하는 Head
- src/java.base/share/classes/java/util/stream/ReferencePipeline.java:901: stateless 중간 연산 기반 클래스
- src/java.base/share/classes/java/util/stream/ReferencePipeline.java:942: stateful 중간 연산 기반 클래스

ReferencePipeline은 추상 클래스이고, 실제 stage는 주로 StatelessOp 또는 StatefulOp의 익명/구체 서브클래스입니다.

———

3. StatelessOp

Stateless 연산은 현재 원소 하나만으로 결과를 결정할 수 있습니다.

filter: 원소 → predicate 검사 → 통과하면 전달
map:    원소 → mapper 적용     → 변환 결과 전달
peek:   원소 → action 실행     → 같은 원소 전달

예를 들어 filter()는 src/java.base/share/classes/java/util/stream/ReferencePipeline.java:201에서 익명 StatelessOp를 만듭니다.

return new StatelessOp<>(this, StreamShape.REFERENCE, NOT_SIZED) {
    Sink<P_OUT> opWrapSink(int flags, Sink<P_OUT> downstream) {
        return new Sink.ChainedReference<>(downstream) {
            public void accept(P_OUT value) {
                if (predicate.test(value))
                    downstream.accept(value);
                }
        };
    }
};

StatelessOp 자체는 opIsStateful()을 false로 고정할 뿐입니다. 실제 연산 내용은 각 익명 클래스의 opWrapSink()가 구현합니다.

———

4. StatefulOp

Stateful 연산은 다른 원소나 처리 순서에 관한 상태가 필요합니다.

- sorted: 모든 원소를 모은 뒤 정렬
- distinct: 이미 본 원소를 추적
- limit, skip: 처리한 원소 수와 encounter order 추적
- takeWhile, dropWhile: 조건이 깨진 위치 추적

예를 들어 sorted()는 SortedOps.makeRef()를 거쳐 src/java.base/share/classes/java/util/stream/SortedOps.java:99의 ReferencePipeline.StatefulOp 구현을 생성합니다.

순차 sorted Sink:

begin()     버퍼 생성
accept(x)   버퍼에 x 저장
end()       버퍼 정렬
정렬된 원소를 downstream.accept()로 전달

StatefulOp은 opIsStateful()을 true로 고정하고, 병렬 평가용 opEvaluateParallel() 구현도 요구합니다.

———

5. 실제 실행 과정

다음 파이프라인을 기준으로 보면:

long result = list.stream()
        .filter(x -> x > 0)
        .map(x -> x * 2)
        .count();

구축 단계에서는 stage 연결만 만들어집니다.

[Head] → [filter stage] → [map stage]
                                │
                                └── 현재 Stream 객체

count()가 호출되면 src/java.base/share/classes/java/util/stream/AbstractPipeline.java:309의 evaluate()가 실행됩니다.

1. Stream을 consumed 상태로 변경
2. Head에서 source Spliterator 확보
3. count용 TerminalOp/TerminalSink 생성
4. 마지막 stage부터 역방향으로 Sink 체인 조립
5. Spliterator에서 원소를 꺼내 Sink 체인에 push
6. TerminalSink에서 결과 반환

stage 체인과 실행용 Sink 체인은 방향이 다르게 조립됩니다.

Stage 구조:
Head ──▶ FilterStage ──▶ MapStage

wrapSink() 조립 순서:
CountSink
    ↓ map이 감쌈
MapSink(CountSink)
    ↓ filter가 감쌈
FilterSink(MapSink(CountSink))

원소 실행 방향:
Spliterator
    │ value
    ▼
FilterSink.accept(value)
    │ 통과한 value
    ▼
MapSink.accept(value)
    │ mapped value
    ▼
CountSink.accept(mappedValue)

Sink 조립은 src/java.base/share/classes/java/util/stream/AbstractPipeline.java:693, 실제 push는 src/java.base/share/classes/java/util/stream/AbstractPipeline.java:631에서 수행됩니다.

———

6. 순차와 병렬의 차이

순차 스트림은 기본적으로 하나의 Sink 체인에 원소를 계속 밀어 넣습니다. Stateful stage가 있어도 같은 체인에 포함되지만, sorted처럼 내부에서 버퍼링한 후 end() 시점에 결과를 내보낼 수 있습니다.

병렬 스트림에서 stateless 연산만 있다면:

source Spliterator 분할
    ├─ partition A → filter → map → terminal
    ├─ partition B → filter → map → terminal
    └─ partition C → filter → map → terminal

중간 stage 사이에 전체 결과를 담는 컬렉션이 필요하지 않습니다.

병렬 스트림에 stateful 연산이 있으면 논리적인 segment로 분리됩니다.

source → filter → map │ sorted │ filter → terminal
         segment 1      경계       segment 2

src/java.base/share/classes/java/util/stream/AbstractPipeline.java:525가 stateful stage를 만나면 opEvaluateParallelLazy()를 호출하고, 그 결과 Spliterator를 다음 segment의 소스로 사용합니다. depth도 해당 경계에서 다시 계산됩니다.

다만 모든 stateful 연산이 반드시 전체 결과를 즉시 배열로 구체화하는 것은 아닙니다. sorted처럼 실제 barrier가 필요한 연산도 있고, unordered distinct처럼 lazy wrapping이 가능한 최적화도 있습니다.

———

핵심을 한 문장으로 요약하면:

> AbstractPipeline stage 체인은 “무슨 연산을 할지” 저장하고, 터미널 연산이 호출되면 각 stage의 opWrapSink()로 실행 체인을 만든 뒤 Spliterator의 원소를 그 체인에 밀어 넣습니다. StatelessOp은 원소별로 바로 전달할 수 있고, StatefulOp은 상태 관리와 특히 병렬 실행에서 segment 경계를 필요로 합니다.


---
Q.
› terminal operation이 실행되면 src/java.base/share/classes/java/util/stream/AbstractPipeline.java evaluate 이 어디로부터 어떻게 호출되는지 궁금합니다. 관련 코드도 찾아주세요. 또, terminalOp.evaluateParallel, terminalOp.evaluateSequential 이 실행되는데 관련 코드는 어디서 볼 수 있을까요?


1. AbstractPipeline.evaluate()는 누가 호출하는가

대부분의 터미널 연산은 ReferencePipeline에 구현되어 있으며, 메서드 본문에서 상속받은 AbstractPipeline.evaluate()를 직접 호출합니다.

예를 들어:

list.stream()
    .filter(x -> x > 0)
    .forEach(System.out::println);

호출 흐름은 다음과 같습니다.

사용자 코드
    │
    │ Stream.forEach(action)
    ▼
ReferencePipeline.forEach(action)
    │
    │ ForEachOps.makeRef(action, false)
    │ TerminalOp 객체 생성
    ▼
AbstractPipeline.evaluate(terminalOp)
    │
    ├─ sequential → terminalOp.evaluateSequential(...)
    └─ parallel   → terminalOp.evaluateParallel(...)

실제 forEach() 구현은 src/java.base/share/classes/java/util/stream/ReferencePipeline.java:674에 있습니다.

@Override
public void forEach(Consumer<? super P_OUT> action) {
    evaluate(ForEachOps.makeRef(action, false));
}

여기서 ReferencePipeline은 AbstractPipeline을 상속하므로, 별도의 특별한 연결 없이 일반적인 상속 메서드 호출로 src/java.base/share/classes/java/util/stream/AbstractPipeline.java:310의 evaluate()가 실행됩니다.

AbstractPipeline
    ▲
    │ extends
ReferencePipeline
    ▲
    │ extends
StatelessOp/StatefulOp/Head

filter()나 map()이 반환한 마지막 stage의 실제 클래스는 익명 ReferencePipeline.StatelessOp이지만, forEach()는 ReferencePipeline에서 상속받고, evaluate()는 다시 AbstractPipeline에서 상속받습니다.

———

2. 터미널 연산별 호출 위치

ReferencePipeline의 대표적인 호출 코드는 다음과 같습니다.

Stream API    TerminalOp 생성                위치
━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
forEach       ForEachOps.makeRef()           src/java.base/share/classes/java/util/stream/ReferencePipeline.java:674
────────────  ─────────────────────────────  ─────────────────────────────────────────────────────────────────────────
anyMatch      MatchOps.makeRef()             src/java.base/share/classes/java/util/stream/ReferencePipeline.java:710
────────────  ─────────────────────────────  ─────────────────────────────────────────────────────────────────────────
findFirst     FindOps.makeRef(true)          src/java.base/share/classes/java/util/stream/ReferencePipeline.java:725
────────────  ─────────────────────────────  ─────────────────────────────────────────────────────────────────────────
reduce        ReduceOps.makeRef()            src/java.base/share/classes/java/util/stream/ReferencePipeline.java:738
────────────  ─────────────────────────────  ─────────────────────────────────────────────────────────────────────────
collect       ReduceOps.makeRef()            src/java.base/share/classes/java/util/stream/ReferencePipeline.java:773
────────────  ─────────────────────────────  ─────────────────────────────────────────────────────────────────────────
count         ReduceOps.makeRefCounting()    src/java.base/share/classes/java/util/stream/ReferencePipeline.java:812

예를 들어 count()는 사실상 다음 호출입니다.

public final long count() {
    return evaluate(ReduceOps.makeRefCounting());
}

min()과 max()는 먼저 reduce()를 호출하고, reduce()가 evaluate()를 호출합니다.

———

3. evaluate() 내부 동작

src/java.base/share/classes/java/util/stream/AbstractPipeline.java:310:

final <R> R evaluate(TerminalOp<E_OUT, R> terminalOp) {
assert getOutputShape() == terminalOp.inputShape();

      if (linkedOrConsumed)
          throw new IllegalStateException(MSG_STREAM_LINKED);

      linkedOrConsumed = true;

      return isParallel()
             ? terminalOp.evaluateParallel(
                     this,
                     sourceSpliterator(terminalOp.getOpFlags()))
             : terminalOp.evaluateSequential(
                     this,
                     sourceSpliterator(terminalOp.getOpFlags()));
}

여기서 중요한 인수는 두 개입니다.

terminalOp.evaluateSequential(
    this,                  // 마지막 pipeline stage, 동시에 PipelineHelper
    sourceSpliterator(...) // 실제 데이터 source
)

this는 마지막 stage입니다. 예를 들어:

Head → filter → map
                ▲
                └── evaluate() 안의 this

마지막 stage는 PipelineHelper이기도 하므로, 터미널 연산은 이 객체를 통해 전체 중간 연산의 Sink 체인을 구성할 수 있습니다.

———

4. TerminalOp 인터페이스

인터페이스 정의는 src/java.base/share/classes/java/util/stream/TerminalOp.java:45에 있습니다.

interface TerminalOp<E_IN, R> {
    default StreamShape inputShape() {
        return StreamShape.REFERENCE;
    }

    default int getOpFlags() {
        return 0;
    }

    default <P_IN> R evaluateParallel(
        PipelineHelper<E_IN> helper,
        Spliterator<P_IN> spliterator) {
        return evaluateSequential(helper, spliterator);
    }

    <P_IN> R evaluateSequential(
        PipelineHelper<E_IN> helper,
        Spliterator<P_IN> spliterator)
    ;
}

evaluateSequential()은 반드시 구현해야 합니다. evaluateParallel()은 기본적으로 순차 구현을 호출하지만, 표준 터미널 연산들은 대부분 병렬 전용 구현을 오버라이드합니다.

———

5. forEach의 구현

ForEachOps.makeRef()는 src/java.base/share/classes/java/util/stream/ForEachOps.java:72에서 ForEachOp.OfRef를 만듭니다.

return new ForEachOp.OfRef<>(action, ordered);

ForEachOp은 TerminalOp과 TerminalSink를 동시에 구현합니다.

abstract static class ForEachOp<T>
    implements TerminalOp<T, Void>, TerminalSink<T, Void>

순차 구현은 src/java.base/share/classes/java/util/stream/ForEachOps.java:151에 있습니다.

public <S> Void evaluateSequential(
    PipelineHelper<T> helper,
    Spliterator<S> spliterator) {
    return helper.wrapAndCopyInto(this, spliterator).get();
}

전체 순차 호출 흐름은 다음과 같습니다.

ReferencePipeline.forEach()
    → AbstractPipeline.evaluate()
        → ForEachOp.evaluateSequential()
            → AbstractPipeline.wrapAndCopyInto()
                → wrapSink(ForEachOp)
                    → copyInto(wrappedSink, spliterator)
                        → spliterator.forEachRemaining(wrappedSink)

병렬 구현은 src/java.base/share/classes/java/util/stream/ForEachOps.java:157에 있습니다.

if (ordered)
    new ForEachOrderedTask<>(helper, spliterator, this).invoke();
else
    new ForEachTask<>(helper, spliterator, helper.wrapSink(this)).invoke();

ForEachTask가 Spliterator.trySplit()으로 작업을 나누는 부분은 src/java.base/share/classes/java/util/stream/ForEachOps.java:281에서 볼 수 있습니다.

———

6. reduce, collect, count의 구현

이 연산들은 주로 ReduceOps.ReduceOp를 사용합니다.

ReduceOp의 순차/병렬 구현은 src/java.base/share/classes/java/util/stream/ReduceOps.java:941에 있습니다.

public <P_IN> R evaluateSequential(
    PipelineHelper<T> helper,
    Spliterator<P_IN> spliterator) {
    return helper.wrapAndCopyInto(makeSink(), spliterator).get();
}

public <P_IN> R evaluateParallel(
    PipelineHelper<T> helper,
    Spliterator<P_IN> spliterator) {
    return new ReduceTask<>(this, helper, spliterator).invoke().get();
}

병렬 reduce의 구조는 다음과 같습니다.

                       ReduceTask
                      /          \
               left task        right task
               부분 결과 A       부분 결과 B
                      \          /
                       combine(A, B)
                             │
                             ▼
                          최종 결과

리프 작업은 src/java.base/share/classes/java/util/stream/ReduceOps.java:1015에서 Sink 체인을 실행합니다.

protected S doLeaf() {
    return helper.wrapAndCopyInto(op.makeSink(), spliterator);
}

결과 병합은 src/java.base/share/classes/java/util/stream/ReduceOps.java:1021입니다.

S leftResult = leftChild.getLocalResult();
leftResult.combine(rightChild.getLocalResult());
setLocalResult(leftResult);

공통적인 Spliterator.trySplit() 분할 알고리즘은 src/java.base/share/classes/java/util/stream/AbstractTask.java:302에 있습니다.

———

7. 단락 터미널 연산

anyMatch, findFirst, findAny는 결과가 나오면 전체 원소를 처리하지 않고 종료할 수 있습니다.

- anyMatch: src/java.base/share/classes/java/util/stream/MatchOps.java:242의 MatchOp
- findFirst/findAny: src/java.base/share/classes/java/util/stream/FindOps.java:131의 FindOp
- 병렬 단락 공통 기반: src/java.base/share/classes/java/util/stream/AbstractShortCircuitTask.java:101

예를 들어 FindOp.evaluateParallel()은 다음과 같습니다.

return new FindTask<>(
    this, mustFindFirst, helper, spliterator
).invoke();

여러 작업 중 하나가 결과를 찾으면 공유된 AtomicReference에 결과를 등록하고 다른 작업들이 중단 여부를 확인합니다.

———

예외적인 경로

모든 터미널 API가 반드시 AbstractPipeline.evaluate(TerminalOp)를 통과하는 것은 아닙니다.

- 중간 연산이 전혀 없는 순차 Head.forEach()는 Spliterator.forEachRemaining()을 직접 호출합니다: src/java.base/share/classes/java/util/stream/ReferencePipeline.java:866
- toArray()는 evaluate() 대신 evaluateToArrayNode() 경로를 사용합니다: src/java.base/share/classes/java/util/stream/ReferencePipeline.java:683
- concurrent collector의 특정 병렬 최적화는 내부적으로 forEach() 경로를 사용합니다.

정리하면 일반적인 핵심 흐름은 다음과 같습니다.

Stream 터미널 API
    → XxxOps.makeXxx()로 TerminalOp 생성
    → AbstractPipeline.evaluate(TerminalOp)
    → isParallel() 분기
    → TerminalOp의 구체 evaluateSequential/evaluateParallel
    → Sink 체인 또는 ForkJoin Task 실행
    → 최종 결과 반환
