코드 보는 순서.

가장 좋은 순서는 “API 표면 → 파이프라인 생성 → 중간 연산 연결 → 터미널 연산 실행 → 병렬/상태ful 처리” 순서입니다. 처음부터 SortedOps, SliceOps, Nodes 같은 복잡한 파일로 들어가면 흐름이 잘 안 잡힙니다.

추천 순서

1. src/java.base/share/classes/java/util/stream/package-info.java:1
   Stream의 설계 의도, lazy evaluation, stateless/stateful operation, short-circuit 개념을 먼저 잡습니다.

lazy evaluation: 최종 연산이 호출 되면 실제 처리 시작
stateless/stateful operation:
short-circuit: input을 전부 읽거나 처리하지 않고 연산이 terminate 되느 것.

2. src/java.base/share/classes/java/util/stream/BaseStream.java:1, src/java.base/share/classes/java/util/stream/Stream.java:1
   사용자가 보는 API입니다. filter, map, reduce, collect, forEach, spliterator가 어떤 계약을 갖는지 봅니다.

3. src/java.base/share/classes/java/util/stream/StreamSupport.java:1
   StreamSupport.stream(...)에서 실제 스트림 객체가 어떻게 만들어지는지 봅니다. 여기서 ReferencePipeline.Head로 이어집니다.

4. src/java.base/share/classes/java/util/stream/ReferencePipeline.java:1
   객체 스트림의 중심 구현입니다. 특히 filter, map, flatMap, forEach, reduce, collect가 어떤 StatelessOp 또는 터미널 연산으로 바뀌는지 보세요.

5. src/java.base/share/classes/java/util/stream/AbstractPipeline.java:1
   핵심입니다. 중간 연산을 호출할 때 파이프라인 stage가 연결되고, 터미널 연산 때 evaluate, wrapSink, copyInto로 실제 데이터가 흐릅니다.

6. src/java.base/share/classes/java/util/stream/Sink.java:1, src/java.base/share/classes/java/util/stream/PipelineHelper.java:1
   Stream 구현의 실행 모델을 이해하는 파일입니다. Sink는 각 stage의 실행 객체이고, wrapSink가 이들을 체인으로 엮습니다.

7. src/java.base/share/classes/java/util/stream/ForEachOps.java:1, src/java.base/share/classes/java/util/stream/ReduceOps.java:1, src/java.base/share/classes/java/util/stream/FindOps.java:1, src/java.base/share/classes/java/util/stream/MatchOps.java:1
   터미널 연산 구현입니다. findFirst, anyMatch 같은 short-circuit 동작도 여기서 감이 잡힙니다.

8. src/java.base/share/classes/java/util/stream/StreamOpFlag.java:1
   ORDERED, SIZED, DISTINCT, SHORT_CIRCUIT 같은 플래그가 최적화와 실행 경로에 어떻게 영향을 주는지 봅니다.

9. src/java.base/share/classes/java/util/stream/SortedOps.java:1, src/java.base/share/classes/java/util/stream/DistinctOps.java:1, src/java.base/share/classes/java/util/stream/SliceOps.java:1, src/java.base/share/classes/java/util/stream/WhileOps.java:1
   stateful intermediate operation입니다. sorted, distinct, limit, skip, takeWhile처럼 원소 하나만 보고는 처리할 수 없는 연산들이 왜 복잡한지 확인할 수 있습니다.

10. src/java.base/share/classes/java/util/stream/AbstractTask.java:1, src/java.base/share/classes/java/util/stream/AbstractShortCircuitTask.java:1, src/java.base/share/classes/java/util/stream/Nodes.java:1
    병렬 스트림 구현입니다. 처음부터 보지 말고 순차 스트림 흐름을 이해한 뒤 들어가는 게 좋습니다.

읽을 때는 이 예제를 머릿속에 두고 따라가면 됩니다.

strings.stream()
.filter(s -> s.startsWith("A"))
.map(String::length)
.reduce(0, Integer::sum);

핵심 질문은 세 개입니다. filter/map을 호출할 때 실제로 무엇이 생성되는가? 터미널 연산이 호출되기 전까지 왜 데이터가 흐르지 않는가? 터미널 연산이 호출되면 Spliterator -> Sink chain -> TerminalSink로 원소가 어떻게 전달되는가? 이 흐름만 잡으면 나머지 파일들은 대부분 특수 케이스로 읽힙니다.

---

java.util.stream은 지연 평가되는 파이프라인을 객체 그래프로 구성한 뒤, 터미널 연산 시 Sink 체인으로 컴파일하여 실행하는 구조입니다.

단순히 GoF 패턴 하나로 만들어졌다기보다 다음 패턴과 실행 기법이 결합되어 있습니다.

Fluent API로 연산 선언
    ↓
AbstractPipeline stage 체인 구성
    ↓ terminal operation
TerminalOp 실행 전략 선택
    ↓
Sink Decorator 체인 생성
    ↓
Spliterator가 원소 공급
    ↓
순차 실행 또는 Fork/Join 분할 실행

## 1. 전체 구조

예를 들어:

long result = strings.stream()
    .filter(s -> !s.isEmpty())
    .map(String::length)
    .count();

내부적으로는 먼저 다음 stage 객체들이 만들어집니다.

ReferencePipeline.Head
    │
    ▼
ReferencePipeline.StatelessOp     filter
    │
    ▼
ReferencePipeline.StatelessOp     map

이때 데이터는 처리되지 않습니다. count()가 호출되어야 실행됩니다.

실행 시에는 stage 체인을 거꾸로 읽어 Sink 체인을 만듭니다.

Stage 구조

Head → FilterStage → MapStage

Sink 조립 구조

FilterSink(
    MapSink(
        CountingSink
    )
)

원소는 다음 방향으로 흐릅니다.

Spliterator
    │
    ▼
FilterSink.accept(value)
    │ predicate 통과
    ▼
MapSink.accept(value)
    │ mapper 적용
    ▼
CountingSink.accept(mappedValue)

---

arrayList, LinkedList, HashMap 등 주요 자료 구조나 Source에서 trySplit 으로 분할하는 핵심 코드들을 보고 싶습니다.


• trySplit() 구현은 자료구조별로 전략이 꽤 다릅니다. 실제 소스에서 ArrayList, LinkedList, HashMap과 비교 기준이 되는 배열/범위 스트림까지 찾아서, “어디를 어떻게 반으로 나누는지” 중심으로 연결해 보겠습니다.

• Explored
└ Search Spliterator<.*> spliterator\(|class ArrayListSpliterator|trySplit\(|class IteratorSpliterator|class KeySpliterator|class ValueSpliterator|class EntrySpliterator|class ArraySpliterator|class IntArraySpliterator|class RangeIntSpliterator|class RangeLongSpliterator in
{ArrayList.java,LinkedList.java,HashMap.java,Spliterators.java}
Read ArrayList.java, LinkedList.java, HashMap.java, Spliterators.java, Streams.java

─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────

• 현재 구현에서 핵심 차이는 이미 드러납니다. ArrayList는 인덱스 범위를 정확히 반분하고, LinkedList는 노드를 바로 반으로 자를 수 없어 앞부분을 배열 배치로 복사하며, HashMap은 원소 수가 아니라 내부 버킷 인덱스 구간을 반분합니다. 이제 이 분할 결과가 병렬 Stream task로 넘어가는 지점까지 함께 확인하겠습니다.

• Explored
└ Read Streams.java
Read AbstractTask.java
Read HashSet.java
Search public Spliterator|class DeqSpliterator|trySplit\( in {ArrayDeque.java,TreeMap.java,HashSet.java,LinkedHashMap.java}
Read Collection.java
Search default Spliterator<E> spliterator|default Stream<E> stream|default Stream<E> parallelStream in Collection.java
Read HashSet.java
Read ArrayDeque.java
Read CopyOnWriteArrayList.java
Read ConcurrentHashMap.java
Read TreeMap.java
Read Collection.java, HashMap.java, AbstractTask.java, ArrayList.java

─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────

• trySplit()은 “현재 Spliterator가 담당하는 원소 일부를 떼어서 새 Spliterator로 반환하고, 자신은 나머지를 담당하도록 변경하는 메서드”입니다.

대부분 구현은 다음 형태입니다.

trySplit() 전
original = [전체 범위]

trySplit() 후
returned = [앞쪽 범위]
original = [뒤쪽 범위]

자료구조의 물리적 구조에 따라 분할 전략이 달라집니다.

Source               분할 기준                       분할 비용    균형
━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━
ArrayList, 배열      인덱스 중간값                        O(1)    정확함
───────────────────  ──────────────────────────  ───────────────  ───────────────────────
LinkedList           노드를 배열 batch로 복사         O(batch)    제한적
───────────────────  ──────────────────────────  ───────────────  ───────────────────────
HashMap              bucket 배열 인덱스                   O(1)    원소 기준 불균형 가능
───────────────────  ──────────────────────────  ───────────────  ───────────────────────
ArrayDeque           원형 배열 논리 거리                  O(1)    정확함
───────────────────  ──────────────────────────  ───────────────  ───────────────────────
TreeMap              트리의 루트/서브트리        O(1)에 가까움    대략적
───────────────────  ──────────────────────────  ───────────────  ───────────────────────
IntStream.range      숫자 범위                            O(1)    매우 좋음
───────────────────  ──────────────────────────  ───────────────  ───────────────────────
일반 Collection      Iterator에서 batch 복사          O(batch)    제한적
───────────────────  ──────────────────────────  ───────────────  ───────────────────────
ConcurrentHashMap    table index 범위                     O(1)    동시 변경 허용

## 1. 병렬 Stream에서 trySplit이 호출되는 위치

먼저 Collection.parallelStream()은 Collection의 Spliterator를 Stream source로 전달합니다.

src/java.base/share/classes/java/util/Collection.java:768

default Stream<E> parallelStream() {
    return StreamSupport.stream(spliterator(), true);
}

병렬 터미널 연산이 실행되면 Fork/Join task가 반복적으로 trySplit()을 호출합니다.

src/java.base/share/classes/java/util/stream/AbstractTask.java:302

public void compute() {
    Spliterator<P_IN> rs = spliterator, ls;
    long sizeEstimate = rs.estimateSize();
    long sizeThreshold = getTargetSize(sizeEstimate);

    while (sizeEstimate > sizeThreshold
          && (ls = rs.trySplit()) != null) {

      task.leftChild  = leftChild  = task.makeChild(ls);
      task.rightChild = rightChild = task.makeChild(rs);

      // 한쪽은 fork하고 다른 쪽은 현재 스레드가 계속 처리
      taskToFork.fork();

      sizeEstimate = rs.estimateSize();
    }

    task.setLocalResult(task.doLeaf());
    task.tryComplete();
}

전체 연결은 다음과 같습니다.

collection.parallelStream()
│
▼
collection.spliterator()
│
▼
TerminalOp.evaluateParallel()
│
▼
ForkJoin Task
│
├─ spliterator.trySplit()
│      ├─ left child
│      └─ right child
│
▼
leaf task에서 Sink 체인 실행

기본 목표는 프로세서 하나당 약 네 개의 leaf task를 만드는 것입니다.

src/java.base/share/classes/java/util/stream/AbstractTask.java:194

private static final int LEAF_TARGET =
ForkJoinPool.getCommonPoolParallelism() << 2;

public static long suggestTargetSize(long sizeEstimate) {
long est = sizeEstimate / getLeafTarget();
return est > 0L ? est : 1L;
}

———

## 2. ArrayList: 인덱스를 정확히 절반으로 분할

src/java.base/share/classes/java/util/ArrayList.java:1636

핵심 상태는 다음 세 필드입니다.

private int index;             // 현재 처리 위치
private int fence;             // 담당 범위의 exclusive end
private int expectedModCount;  // 구조 변경 검사

trySplit()은 매우 단순합니다.

src/java.base/share/classes/java/util/ArrayList.java:1688

public ArrayListSpliterator trySplit() {
int hi = getFence();
int lo = index;
int mid = (lo + hi) >>> 1;

      return (lo >= mid)
              ? null
              : new ArrayListSpliterator(
                      lo,
                      index = mid,
                      expectedModCount);
}

크기가 16이라면:

최초
original = [0, 16)

1회 분할
returned = [0, 8)
original = [8, 16)

각각 다시 분할
[0, 4) [4, 8) [8, 12) [12, 16)

index = mid가 중요한 부분입니다.

new ArrayListSpliterator(lo, index = mid, expectedModCount)

한 문장 안에서:

1. 반환할 Spliterator는 [lo, mid)를 담당
2. 기존 Spliterator의 index는 mid로 이동
3. 기존 Spliterator는 [mid, hi)를 담당

배열 기반이라 임의 인덱스 접근이 가능하므로 분할 자체는 O(1)이고 매우 균등합니다.

또한 첫 사용 시점까지 fence와 expectedModCount 설정을 미룹니다.

private int getFence() {
int hi;
if ((hi = fence) < 0) {
expectedModCount = modCount;
hi = fence = size;
}
return hi;
}

이것이 late binding입니다. Spliterator 생성과 실제 평가 사이의 변경을 가능한 한 늦게 반영합니다.

특성은 다음과 같습니다.

public int characteristics() {
return Spliterator.ORDERED
| Spliterator.SIZED
| Spliterator.SUBSIZED;
}

- ORDERED: 리스트 순서가 있음
- SIZED: 전체 크기를 정확히 앎
- SUBSIZED: 분할된 자식도 정확한 크기를 앎

———

## 3. 배열: ArrayList와 거의 같은 방식

배열은 src/java.base/share/classes/java/util/Spliterators.java:939를 사용합니다.

src/java.base/share/classes/java/util/Spliterators.java:1005

public Spliterator<T> trySplit() {
int lo = index;
int mid = (lo + fence) >>> 1;

      if (lo >= mid)
          return null;

      return new ArraySpliterator<>(
              array,
              lo,
              index = mid,
              characteristics);
}

기본 개념은 ArrayList와 같습니다.

Object[] 또는 primitive[]
│
├─ [lo, mid) 반환
└─ [mid, fence) 기존 객체가 유지

Arrays.stream(array).parallel()과 CopyOnWriteArrayList가 대표적으로 이 구조를 활용합니다.

CopyOnWriteArrayList는 내부 배열의 snapshot을 그대로 배열 Spliterator에 넘깁니다.

src/java.base/share/classes/java/util/concurrent/CopyOnWriteArrayList.java:1170

public Spliterator<E> spliterator() {
return Spliterators.spliterator(
getArray(),
Spliterator.IMMUTABLE | Spliterator.ORDERED);
}

따라서 분할 중 동시 수정 검사나 lock이 필요하지 않습니다.

———

## 4. LinkedList: 노드를 배열 batch로 복사

src/java.base/share/classes/java/util/LinkedList.java:1190

연결 리스트에는 중간 인덱스로 즉시 이동할 방법이 없습니다.

node0 → node1 → node2 → node3 → ...

절반 위치를 찾으려면 노드를 직접 따라가야 합니다. JDK는 한 번에 정확히 절반을 찾는 대신, 앞쪽 노드들을 임시 배열로 복사해 반환합니다.

src/java.base/share/classes/java/util/LinkedList.java:1221

public Spliterator<E> trySplit() {
Node<E> p;
int s = getEst();

      if (s > 1 && (p = current) != null) {
          int n = batch + BATCH_UNIT;

          if (n > s)
              n = s;
          if (n > MAX_BATCH)
              n = MAX_BATCH;

          Object[] a = new Object[n];
          int j = 0;

          do {
              a[j++] = p.item;
          } while ((p = p.next) != null && j < n);

          current = p;
          batch = j;
          est = s - j;

          return Spliterators.spliterator(
                  a, 0, j, Spliterator.ORDERED);
      }

      return null;
}

배치 크기는 점점 커집니다.

static final int BATCH_UNIT = 1 << 10; // 1,024
static final int MAX_BATCH  = 1 << 25;

분할 과정은 대략 다음과 같습니다.

LinkedList 원본

N0 → N1 → ... → N9999
│
│ 첫 trySplit(): 최대 1,024개 복사
▼
array[0..1024) 반환
원본 current는 N1024로 이동

두 번째 trySplit(): 최대 2,048개 복사
▼
array[0..2048) 반환
원본 current는 N3072로 이동

반환된 임시 배열은 다시 O(1) 인덱스 반분이 가능합니다.

LinkedList
│ batch 복사
▼
Object[] Spliterator
│
├─ 배열 절반
└─ 배열 절반

따라서 LinkedList 병렬 처리는 다음 비용을 가집니다.

- 노드를 순회하는 포인터 추적 비용
- 임시 배열 할당 비용
- 노드 원소를 배열에 복사하는 비용
- 낮은 메모리 지역성

그래서 같은 데이터 양이라면 일반적으로 ArrayList가 병렬 Stream source에 훨씬 유리합니다.

———

## 5. 일반 Collection: IteratorSpliterator

Collection이 전용 Spliterator를 구현하지 않으면 기본 구현을 사용합니다.

src/java.base/share/classes/java/util/Collection.java:728

default Spliterator<E> spliterator() {
return Spliterators.spliterator(this, 0);
}

이 경로는 src/java.base/share/classes/java/util/Spliterators.java:1827로 이어집니다.

핵심 trySplit()은 LinkedList와 유사합니다.

src/java.base/share/classes/java/util/Spliterators.java:1890

public Spliterator<T> trySplit() {
Iterator<? extends T> i;
long s;

      if ((i = it) == null) {
          i = it = collection.iterator();
          s = est = collection.size();
      } else {
          s = est;
      }

      if (s > 1 && i.hasNext()) {
          int n = batch + BATCH_UNIT;

          if (n > s)
              n = (int) s;
          if (n > MAX_BATCH)
              n = MAX_BATCH;

          Object[] a = new Object[n];
          int j = 0;

          do {
              a[j] = i.next();
          } while (++j < n && i.hasNext());

          batch = j;
          est -= j;

          return new ArraySpliterator<>(
                  a, 0, j, characteristics);
      }

      return null;
}

Iterator는 임의 위치로 이동할 수 없으므로, 역시 batch를 배열로 옮겨 병렬성을 얻습니다.

———

## 6. HashMap: bucket 인덱스 범위를 반분

HashMap 자체가 Stream을 제공하는 것은 아니며, 보통 view를 통해 사용합니다.

map.keySet().parallelStream()
map.values().parallelStream()
map.entrySet().parallelStream()

각 view는 전용 Spliterator를 생성합니다.

src/java.base/share/classes/java/util/HashMap.java:997

public final Spliterator<K> spliterator() {
return new KeySpliterator<>(
HashMap.this, 0, -1, 0, 0);
}

공통 상태는 src/java.base/share/classes/java/util/HashMap.java:1640에 있습니다.

final HashMap<K,V> map;
Node<K,V> current;

int index;  // 현재 bucket index
int fence;  // 마지막 bucket index
int est;    // 원소 수 추정치

KeySpliterator.trySplit()은 bucket index 구간을 반으로 나눕니다.

src/java.base/share/classes/java/util/HashMap.java:1689

public KeySpliterator<K,V> trySplit() {
int hi = getFence();
int lo = index;
int mid = (lo + hi) >>> 1;

      return (lo >= mid || current != null)
              ? null
              : new KeySpliterator<>(
                      map,
                      lo,
                      index = mid,
                      est >>>= 1,
                      expectedModCount);
}

예를 들어 table 길이가 16이라면:

HashMap.table

bucket index:
0 1 2 3 4 5 6 7 │ 8 9 10 11 12 13 14 15
─────────────────┼────────────────────────
returned [0, 8)  │ original [8, 16)

중요한 점은 원소 개수가 아니라 bucket index를 반분한다는 것입니다.

bucket 0 : 원소 0개
bucket 1 : 원소 0개
bucket 2 : 원소 10개
bucket 3 : 원소 0개
...

이런 분포라면 bucket 범위를 반으로 나누더라도 실제 작업량은 균등하지 않을 수 있습니다.

est >>>= 1 역시 실제 원소를 세어 반으로 나누는 것이 아니라 추정치를 절반으로 줄이는 것입니다.

또한:

current != null

이면 더 이상 분할하지 않습니다. 이미 특정 bucket의 연결 구조를 순회하기 시작했다면 그 bucket 내부에서 다시 쪼개지 않습니다.

ValueSpliterator와 EntrySpliterator도 같은 방식이며, 반환 값만 각각 value, entry로 다릅니다.

HashSet은 내부적으로 HashMap을 사용하므로 똑같은 분할 전략을 재사용합니다.

src/java.base/share/classes/java/util/HashSet.java:371

public Spliterator<E> spliterator() {
return new HashMap.KeySpliterator<>(
map, 0, -1, 0, 0);
}

———

## 7. ArrayDeque: 원형 배열의 논리적 거리를 반분

ArrayDeque는 원형 배열을 사용합니다.

실제 배열
[ E4 E5 · · · E0 E1 E2 E3 ]
tail  head

단순히 (head + tail) / 2를 사용할 수 없기 때문에 원형 배열상의 논리적 거리를 먼저 계산합니다.

src/java.base/share/classes/java/util/ArrayDeque.java:798

src/java.base/share/classes/java/util/ArrayDeque.java:825

public DeqSpliterator trySplit() {
final Object[] es = elements;
final int i, n;

      return ((n = sub(
                  getFence(),
                  i = cursor,
                  es.length) >> 1) <= 0)
              ? null
              : new DeqSpliterator(
                      i,
                      cursor = inc(i, n, es.length));
}

개념적으로는:

n = cursor부터 fence까지의 원형 거리
mid = n / 2

returned = cursor부터 mid까지
original.cursor = mid

배열 기반이므로 분할 비용은 O(1)입니다.

———

## 8. IntStream.range: 숫자 범위를 직접 분할

IntStream.range(0, n)은 컬렉션이나 배열 없이 숫자 범위 자체를 source로 사용합니다.

src/java.base/share/classes/java/util/stream/Streams.java:54

src/java.base/share/classes/java/util/stream/Streams.java:131

public Spliterator.OfInt trySplit() {
long size = estimateSize();

      return size <= 1
              ? null
              : new RangeIntSpliterator(
                      from,
                      from = from + splitPoint(size),
                      0);
}

일반적인 크기는 정확히 절반으로 나눕니다.

private int splitPoint(long size) {
int d = (size < BALANCED_SPLIT_THRESHOLD)
? 2
: RIGHT_BALANCED_SPLIT_RATIO;

      return (int) (size / d);
}

다만 매우 큰 범위에서는 1:7 비율로 나눕니다.

private static final int BALANCED_SPLIT_THRESHOLD = 1 << 24;
private static final int RIGHT_BALANCED_SPLIT_RATIO = 1 << 3;

보통 범위
[0................100)
→ [0...50) + [50...100)

매우 큰 범위
[0................................................n)
→ 앞쪽 약 1/8 + 뒤쪽 약 7/8

이는 거대한 IntStream.range(...).limit(...) 같은 파이프라인에서 앞쪽 범위를 낮은 task 깊이에서 먼저 처리하기 위한 최적화입니다.

———

## 9. TreeMap: 트리 구조를 이용한 분할

src/java.base/share/classes/java/util/TreeMap.java:3073는 배열 인덱스가 없으므로 트리의 루트나 서브트리 노드를 분할 지점으로 선택합니다.

TreeMap.Entry<K,V> s =
(e == null || e == f) ? null :
(side == 0)           ? tree.root :
(side > 0)            ? e.right :
(side < 0 && f != null) ? f.left :
null;

                root
               /    \
         left subtree  right subtree

최상위에서는 root를 기준으로 분할하고, 이후에는 현재 어느 쪽을 순회 중인지에 따라 right 또는 fence의 left를 후보로 사용합니다.

정렬 순서는 유지되지만 균형이 완벽하다는 보장은 없습니다. TreeMap 트리 자체는 Red-Black Tree라 높이는 균형을 이루지만, 각 서브트리의 정확한 원소 수를 별도로 저장하지 않기 때문입니다.

———

## 10. ConcurrentHashMap

src/java.base/share/classes/java/util/concurrent/ConcurrentHashMap.java:3582도 table index 범위를 반분합니다.

public KeySpliterator<K,V> trySplit() {
int i, f, h;

      return (h = ((i = baseIndex)
                 + (f = baseLimit)) >>> 1) <= i
              ? null
              : new KeySpliterator<>(
                      tab,
                      baseSize,
                      baseLimit = h,
                      f,
                      est >>>= 1);
}

HashMap과 기본 분할 개념은 비슷하지만 특성이 다릅니다.

public int characteristics() {
return Spliterator.DISTINCT
| Spliterator.CONCURRENT
| Spliterator.NONNULL;
}

- fail-fast 대신 weakly consistent 순회
- 순회 중 동시 변경 허용
- table resize까지 Traverser가 처리
- 정확한 SIZED 보장은 하지 않음

———

## 핵심 비교

ArrayList
[index................fence]
↓ midpoint
[left half] [right half]
O(1), 정확한 크기

LinkedList
node → node → node → ...
↓ 앞쪽 노드를 배열에 복사
Object[] batch
O(batch), 추가 메모리 필요

HashMap
table[0................15]
↓ midpoint
bucket[0..8) bucket[8..16)
O(1), 실제 원소 수는 불균형 가능

IntStream.range
[from................upTo)
↓ numeric split point
[left range] [right range]
O(1), 가장 이상적인 source 중 하나

병렬 Stream source로서의 일반적인 분할 적합성은 다음처럼 볼 수 있습니다.

매우 좋음
배열 ≈ ArrayList ≈ IntStream.range
> ArrayDeque
> HashMap / ConcurrentHashMap
> TreeMap
> LinkedList / 일반 Iterator 기반 Collection
상대적으로 제한적

다만 실제 성능은 trySplit()뿐 아니라 원소당 연산 비용, 데이터 크기, 메모리 지역성, encounter order, stateful 연산 유무에 더 크게 좌우됩니다. 작은 컬렉션이나 가벼운 연산에서는 분할과 Fork/Join 비용 때문에 순차 Stream이 더 빠를 수 있습니다