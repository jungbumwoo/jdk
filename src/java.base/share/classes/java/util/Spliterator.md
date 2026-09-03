Java Spliterator 구현체 분석

1. ArrayList - ArrayListSpliterator

파일: ArrayList.java:1636

핵심 필드:
private int index;             // 현재 위치
private int fence;             // -1로 시작, 실제 범위 끝 index
private int expectedModCount;  // 구조 변경 감지용

Lazy Initialization 전략: fence가 -1일 때 getFence()가 처음 호출되는 시점에 modCount를 캡처합니다. spliterator 생성 시점이 아닌, 실제 사용 시점까지 초기화를 지연합니다.

trySplit()   → (lo + hi) >>> 1  (비트 shift로 중간값, 정확히 절반으로 분할)
→ 왼쪽 절반 새 spliterator 반환, this는 오른쪽 절반 담당

forEachRemaining 최적화:
- elementData 배열 참조를 로컬 변수 a에 hoisting
- loop 안에서 매번 필드 접근하지 않도록 최적화
- modCount 검사는 루프 완료 후 1회만 (tryAdvance는 매 원소마다 체크)

characteristics: ORDERED | SIZED | SUBSIZED

  ---
2. LinkedList - LLSpliterator

파일: LinkedList.java:1189

LinkedList는 인덱스 기반 접근이 불가능하므로 배열 배치(batch) 전략 사용:

static final int BATCH_UNIT = 1 << 10;  // 1024 (분할 시 배치 증가 단위)
static final int MAX_BATCH  = 1 << 25;  // 33,554,432 (최대 배치 크기)

trySplit() 핵심 로직:
// 노드를 임시 Object[] 배열에 복사 후 ArraySpliterator로 반환
Object[] a = new Object[n];
do { a[j++] = p.item; } while ((p = p.next) != null && j < n);
current = p;                      // this spliterator는 남은 노드부터
return Spliterators.spliterator(a, 0, j, ORDERED);  // 분리된 절반

LinkedList를 병렬 처리할 때 trySplit이 노드를 배열로 변환하는 비용이 발생합니다. ArrayList 대비 병렬 처리 효율이 낮은 이유입니다.

characteristics: ORDERED | SIZED | SUBSIZED

  ---
3. HashMap - HashMapSpliterator 계층

파일: HashMap.java:1645

HashMapSpliterator (base) → KeySpliterator, ValueSpliterator, EntrySpliterator

getFence(): bucket 배열(table)의 length를 fence로 사용

hi = fence = (tab == null) ? 0 : tab.length;

trySplit() (KeySpliterator):
// bucket index 기준으로 절반 분할
return (lo >= mid || current != null) ? null :
new KeySpliterator<>(map, lo, index = mid, est >>>= 1, ...);
- current != null 조건: chain 중간에 있으면 분할 불가
- est >>>= 1: size 추정값도 절반으로

characteristics (Key):
return (fence < 0 || est == map.size ? Spliterator.SIZED : 0) | Spliterator.DISTINCT;
- Key는 DISTINCT 보장
- Value는 DISTINCT 없음
- SUBSIZED 없음 - bucket 분할이라 정확한 크기 보장 불가

  ---
구조 비교 요약

|           | ArrayList      | LinkedList     | HashMap (Key)     |
  |-----------|----------------|----------------|-------------------|
| 분할 방식 | index 이분     | node→배열 변환 | bucket index 이분 |
| 분할 비용 | O(1)           | O(n) 복사      | O(1)              |
| SIZED     | O              | O              | 조건부            |
| SUBSIZED  | O              | O              | X                 |
| DISTINCT  | X              | X              | O (Key)           |
| ORDERED   | O              | O              | X                 |
| CME 감지  | forEach 후 1회 | 각 원소마다    | 각 원소마다       |

  ---
핵심 설계 패턴

1. Lazy Initialization: fence = -1 → 첫 사용 시 modCount 캡처. spliterator 생성 후 리스트 크기가 변해도 안전하게 처리
2. Fail-fast: modCount 비교로 구조 변경 감지 후 ConcurrentModificationException
3. 배열 hoisting (forEachRemaining): 필드 접근을 루프 전 로컬 변수에 저장 → JIT 최적화 용이
4. LinkedList의 배치 전략: 포인터 추적 비용을 배열 변환으로 상쇄, BATCH_UNIT씩 점진적으로 증가
