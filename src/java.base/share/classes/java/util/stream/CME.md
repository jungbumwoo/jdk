ConcurrentModificationException

일반 컬렉션
ArrayList / LinkedList / HashMap / TreeMap
→ split lock 없음
→ 동시 수정 지원 안 함
→ modCount 기반 best-effort CME

CopyOnWriteArrayList
→ split lock 없음
→ immutable snapshot 순회
→ 변경 내용은 현재 Stream에 미반영

ConcurrentHashMap
→ split lock 없음
→ write는 CAS 또는 bucket 단위 동기화
→ weakly consistent 순회

배열
→ split lock도 변경 검사도 없음
→ 호출자가 변경하지 않아야 함

가장 안전한 원칙은 일반 컬렉션의 Stream 터미널 연산이 끝날 때까지 source를 변경하지 않는 것입니다. 동시 변경이 필요하다면 snapshot, CopyOnWriteArrayList, ConcurrentHashMap
처럼 명시적으로 그 상황을 지원하는 source를 선택해야 합니다.
