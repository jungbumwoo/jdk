Q.  java fork join pool 이 어떻게 동작하는지, 어떻게 스레드들이 관리되고 os thread와 mapping되는지 궁금합니다.

A.
submit → 큐 적재 → signalWork → Java Thread 생성/start → runWorker → steal/execute → idle park/unpark → 보상 worker 흐름

코드상 ForkJoinWorkerThread는 가상 스레드가 아니라 Thread를 상속한 플랫폼 스레드입니다. 따라서 HotSpot에서는 worker 객체 하나가 JavaThread 하나를 거쳐 네이티브 OS 스레드 하나
로 생성됩니다.

핵심 흐름은 다음과 같습니다.

1. fork()/submit()이 worker 로컬 deque 또는 외부 submission queue에 작업을 넣습니다.
2. worker 자신은 top에서 LIFO로 처리하고, 다른 worker는 base에서 FIFO로 훔칩니다.
3. signalWork()는 idle worker를 unpark하거나 부족할 때 새 worker를 생성합니다.
4. 각 worker는 하나의 장수 platform thread이며, runWorker() 루프에서 여러 task를 계속 실행합니다.
5. 일이 없으면 OS thread를 제거하지 않고 우선 park하며, 필요하면 timeout 후 정리합니다.
6. worker가 join/managed blocking 상태에 들어가면 tryCompensate()가 idle worker를 깨우거나 spare worker를 만듭니다.
7. HotSpot에서는 ForkJoinWorkerThread → JavaThread → native OS thread가 1:1로 연결됩니다.

주요 주석 위치:

- WorkQueue, stealing, worker 관리: src/java.base/share/classes/java/util/concurrent/ForkJoinPool.java:1257
- worker 생성과 Thread.start(): src/java.base/share/classes/java/util/concurrent/ForkJoinPool.java:1836
- idle worker 재개/생성: src/java.base/share/classes/java/util/concurrent/ForkJoinPool.java:1971
- work-stealing 실행 루프: src/java.base/share/classes/java/util/concurrent/ForkJoinPool.java:2080
- park와 worker timeout: src/java.base/share/classes/java/util/concurrent/ForkJoinPool.java:2212
- blocking 보상: src/java.base/share/classes/java/util/concurrent/ForkJoinPool.java:2316
- worker 생명주기: src/java.base/share/classes/java/util/concurrent/ForkJoinWorkerThread.java:66
- Java→HotSpot 경계: src/java.base/share/classes/java/lang/Thread.java:1470, src/hotspot/share/prims/jvm.cpp:2711
- 실제 OS thread 생성: src/hotspot/share/runtime/javaThread.cpp:652, src/hotspot/os/bsd/os_bsd.cpp:674, src/hotspot/os/linux/os_linux.cpp:1122, src/hotspot/os/windows/
  os_windows.cpp:749

----

Q. ForkJoinPool이 아닌 common pool은 언제, 어떻게, 어디서 생성되고 관리되는지?

common pool도 ForkJoinPool입니다. 별도 타입이 아니라 JVM 프로세스 전체에서 공유되는 정적 싱글턴 인스턴스입니다.

생성과 관리 흐름은 다음과 같습니다.

1. ForkJoinPool 클래스가 처음 초기화됩니다.
2. 정적 초기화 블록에서 new ForkJoinPool((byte) 0)이 정확히 한 번 실행됩니다.
3. 이때 pool 설정과 큐 자료구조만 생성되고 worker thread는 아직 생성되지 않습니다.
4. 작업이 제출되면 WorkQueue.push()가 signalWork()를 호출합니다.
5. signalWork()는 유휴 worker를 깨우거나 createWorker()로 새 worker를 지연 생성합니다.
6. worker는 runWorker()에서 자기 큐를 처리하거나 다른 큐의 작업을 steal합니다.
7. 유휴 worker는 awaitWork()에서 대기하고, 필요하면 keep-alive 정책에 따라 정리됩니다.
8. common pool에는 shutdown()과 shutdownNow()가 적용되지 않으며 JVM 종료까지 유지됩니다.

주요 주석 위치:

- 전체 생성·관리 흐름: src/java.base/share/classes/java/util/concurrent/ForkJoinPool.java:1706
- 실제 worker 생성: src/java.base/share/classes/java/util/concurrent/ForkJoinPool.java:1848
- worker 활성화 및 지연 생성: src/java.base/share/classes/java/util/concurrent/ForkJoinPool.java:1983
- 유휴 worker 관리: src/java.base/share/classes/java/util/concurrent/ForkJoinPool.java:2224
- common pool 전용 생성자: src/java.base/share/classes/java/util/concurrent/ForkJoinPool.java:3169
- 싱글턴 조회: src/java.base/share/classes/java/util/concurrent/ForkJoinPool.java:3239
- shutdown() 무시 처리: src/java.base/share/classes/java/util/concurrent/ForkJoinPool.java:4188
- 실제 정적 생성 코드: src/java.base/share/classes/java/util/concurrent/ForkJoinPool.java:4518

기본 parallelism은 별도 시스템 프로퍼티가 없으면 일반적으로 availableProcessors() - 1이며, 아래 프로퍼티로 설정할 수 있습니다.

- java.util.concurrent.ForkJoinPool.common.parallelism
- java.util.concurrent.ForkJoinPool.common.maximumSpares
- java.util.concurrent.ForkJoinPool.common.threadFactory
- java.util.concurrent.ForkJoinPool.common.exceptionHandler