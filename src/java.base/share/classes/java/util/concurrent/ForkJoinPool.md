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