# task-executor

멀티프로세스 작업 실행기. 다른 쪽(producer)에서 큐에 작업을 넣으면, 워커 프로세스가
꺼내서 실행한다.

## 요구 사항

- Python >= 3.11

## 설치

```bash
pip install git+https://github.com/WsuRagDocumentAssistant/TaskExecutor.git
```

## 사용법

작업(task)은 인자 없이 호출 가능한 객체여야 하고, **pickle 가능해야 한다.**
Windows는 `spawn` 방식이므로 lambda나 지역 함수는 큐로 넘길 수 없다.
모듈 최상위에 정의된 함수/클래스를 사용한다.

```python
from multiprocessing import Queue

from taskexecutor import TaskExecutionError, TaskExecutorProcess


def my_task():            # 모듈 최상위 정의 (pickle 가능)
    return "작업 결과"


if __name__ == "__main__":   # Windows에서는 필수
    task_queue = Queue()

    worker = TaskExecutorProcess(task_queue)
    worker.start()

    # producer 쪽: 작업을 만들어 큐에 넣는다
    for _ in range(5):
        task_queue.put(my_task)

    # 더 이상 넣을 작업이 없으면 종료 신호를 보낸다.
    # 큐에 남아있는 작업은 모두 처리한 뒤 워커가 종료된다.
    worker.stop()

    results = worker.collect()   # join() 전에 결과를 비운다
    worker.join()

    for item in results:
        if isinstance(item, TaskExecutionError):
            print("실패:", item)
        else:
            print("성공:", item)
```

### 결과 받기

작업의 반환값과 실패 정보는 모두 `result_queue`로 온다. 실패한 작업은
`TaskExecutionError`으로 감싸져 오므로 `isinstance`로 구분한다.

- `collect()` — 워커가 종료될 때까지 기다리며 결과를 모두 모은다. 종료 시 사용.
- `get_task_result(timeout=None)` — 결과를 하나만 꺼낸다. 꺼낼 개수를 알 때 사용.

`get_task_result()`는 기본적으로 결과가 올 때까지 무한 대기한다. 올 결과가 없으면
영원히 멈추므로, 제출한 작업 수만큼만 부르거나 `timeout`을 주어야 한다. `timeout`을
주면 그 시간 안에 결과가 없을 때 `queue.Empty`가 발생한다.

**`join()` 전에 반드시 결과 큐를 비워야 한다.** 큐에 데이터를 넣은 프로세스는
버퍼가 파이프로 다 빠져나갈 때까지 종료되지 못한다. 비우지 않고 `join()`하면
자식은 flush를 기다리고 부모는 자식을 기다리는 교착에 빠진다. 결과가 적을 때는
파이프 버퍼에 다 들어가서 멀쩡히 돌다가, 데이터가 커지면 그때 멈춘다.

### 종료에 대해

`TaskExecutorProcess`는 non-daemon 프로세스다. 종료 신호를 보내지 않으면 워커가 큐에서
계속 대기하므로 **부모 프로세스도 종료되지 않는다.** 반드시 `stop()`으로 마무리한다.

워커를 여러 개 띄웠다면 워커 수만큼 종료 신호가 필요하다. 신호 하나는 워커 하나만
꺼내 가기 때문이다.

```python
workers = [TaskExecutorProcess(task_queue) for _ in range(4)]
for w in workers:
    w.start()

...

for w in workers:
    w.stop()          # 워커 1개당 신호 1개
for w in workers:
    results += w.collect()
for w in workers:
    w.join()
```

`terminate()`는 실행 중인 작업을 중간에 끊고 큐에 남은 작업도 버리므로, 정상 종료
경로로 쓰지 않는다.

### 작업 실패

task 내부에서 예외가 발생하면 워커는 죽지 않는다. traceback을 로그로 남기고,
`TaskExecutionError`으로 감싸 결과 큐에 넣고, 다음 작업으로 넘어간다.
예외를 부모 프로세스로 던지지는 않는다 — `raise`는 프로세스 경계를 넘지 못하며,
`run()` 밖으로 예외가 새어나가면 워커만 조용히 죽고 부모는 그 사실을 알지 못한다.

`TaskExecutionError`은 원본 예외 객체가 아니라 작업 이름과 traceback 문자열만
담는다. 원본 예외는 그 자체가 pickle 불가능할 수 있어서, 실패를 알리려던 통로가
다시 실패하게 된다.

단, **반환값은 pickle 가능해야 한다.** `Queue.put()`은 버퍼에 넣고 바로 리턴하고
직렬화는 뒤에서 feeder 스레드가 하기 때문에, 반환값을 보낼 수 없으면 예외가 나지
않고 그 결과가 조용히 사라진다. 로그에는 `Task Finished`가 남지만 부모는 아무것도
받지 못한다.

유실 범위는 그 결과 하나가 아니다. feeder 스레드가 직렬화 실패로 루프를 벗어나기
때문에, 타이밍에 따라 **뒤이어 보낸 결과들까지 함께 버려진다.** 실측에서 6회 중
3회는 뒤에 있던 결과 3개가 통째로 사라졌다. 작업은 평범한 데이터만 반환해야 한다.
