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

작업(task)은 **`params` 속성을 가진 호출 가능한 객체**여야 한다. 워커는 이것을
`task(task.params)` 로 실행한다. 그리고 **pickle 가능해야 한다** — Windows는 `spawn`
방식이므로 lambda나 지역 함수는 큐로 넘길 수 없고, 모듈 최상위에 정의된 클래스를 쓴다.

작업 큐와 결과 큐는 `TaskExecutor` 가 직접 만든다. `get_task_queue()` /
`get_result_queue()` 로 꺼내 쓴다.

```python
from taskexecutor import TaskExecutionError, TaskExecutor


class MyTask:                     # 모듈 최상위 정의 (pickle 가능)
    def __init__(self, params):
        self.params = params

    def __call__(self, params):
        return f"작업 결과: {params['job_id']}"


if __name__ == "__main__":        # Windows에서는 필수
    worker = TaskExecutor()
    task_queue = worker.get_task_queue()
    worker.start()

    # producer 쪽: 작업을 만들어 큐에 넣는다
    for i in range(5):
        task_queue.put(MyTask({"job_id": i}))

    # 더 이상 넣을 작업이 없으면 종료 신호를 보낸다.
    # 큐에 남아있는 작업은 모두 처리한 뒤 워커가 종료된다.
    worker.stop()

    results = worker.collect()    # join() 전에 결과를 비운다
    worker.join()

    for task, result in results:
        if isinstance(result, TaskExecutionError):
            print(f"실패 (job_id={task.params['job_id']}):", result)
        else:
            print(f"성공 (job_id={task.params['job_id']}):", result)
```

### 실행 순서

기본(`max_workers=1`)은 작업 하나를 끝까지 실행한 뒤에 다음 것을 큐에서 꺼낸다.
**오래 걸리는 작업이 있으면 뒤에 있는 작업들은 그동안 대기한다.** 3초짜리 작업
뒤에 즉시 끝나는 작업 3개를 넣으면, 그 3개도 3초 뒤에야 결과가 나온다.

### 동시 실행

`max_workers` 를 2 이상으로 주면 워커 프로세스 안에서 스레드풀을 만들고, 큐에서
꺼낸 작업을 풀에 던진 뒤 곧바로 다음 것을 꺼낸다. 오래 걸리는 작업이 뒤의 작업을
막지 않는다.

```python
worker = TaskExecutor(max_workers=4)
```

기본값 1이면 스레드풀을 아예 만들지 않으므로 기존 사용처는 그대로 순차 동작한다.

**결과는 작업 순서대로 오지 않는다.** 먼저 끝난 것부터 결과 큐에 들어가므로,
받는 쪽은 결과에 실려 온 `task.params` 로 어느 요청의 결과인지 짝을 맞춰야 한다.

스레드풀이라 GIL의 제약을 받는다. I/O 대기(파일·네트워크·DB)나 계산 중 GIL을
놓는 네이티브 연산에는 효과가 있지만, 순수 파이썬 계산에는 효과가 없다.

### 결과 받기

성공이든 실패든 `(task, 결과)` 한 쌍으로 결과 큐에 온다. 실패는 결과 자리에
`TaskExecutionError` 가 들어오므로 `isinstance` 로 구분한다. 실패에도 `task` 가
실려 있어서, 받는 쪽은 `task.params` 로 어느 요청의 결과인지 짝지을 수 있다.
`max_workers > 1` 이면 결과 순서가 제출 순서와 다르므로 이 짝맞추기가 필수다.

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

`TaskExecutor`는 non-daemon 프로세스다. 종료 신호를 보내지 않으면 워커가 큐에서
계속 대기하므로 **부모 프로세스도 종료되지 않는다.** 반드시 `stop()`으로 마무리한다.

`max_workers > 1` 인 경우 `stop()` 신호를 받으면 새 작업은 더 받지 않고, 스레드에서
아직 돌고 있는 작업이 결과를 다 넣을 때까지 기다린 뒤 종료한다. "쌓인 작업은 다
처리한 뒤 종료"라는 의미는 순차 동작과 같다.

워커 프로세스를 여러 개 띄워 큐를 공유하는 것도 가능하다. 이때는 워커 수만큼 종료
신호가 필요하다 — 신호 하나는 워커 하나만 꺼내 가기 때문이다.

```python
workers = [TaskExecutor() for _ in range(4)]
shared_task_queue = workers[0].get_task_queue()
shared_result_queue = workers[0].get_result_queue()
for w in workers[1:]:                 # start() 전에 바꿔 끼우면 자식에게 함께 넘어간다
    w.task_queue = shared_task_queue
    w.result_queue = shared_result_queue
for w in workers:
    w.start()

...

for w in workers:
    w.stop()          # 워커 1개당 신호 1개

results = workers[0].collect(workers)   # 공유 큐이므로 워커 전부를 넘긴다
for w in workers:
    w.join()
```

`collect()`는 넘겨받은 워커가 모두 끝날 때까지 기다린다. 공유 큐에서 자기 하나만
보면 다른 워커가 아직 실행 중인데도 반환해 결과를 놓치고, 그 워커의 `join()`이
멈춘다.

단, 프로세스를 늘리면 메모리가 따로다. 무거운 모델을 로드하는 작업이라면 프로세스
수만큼 모델이 중복 적재되므로, 그런 경우엔 `max_workers` 로 스레드를 늘리는 쪽이 맞다.

`terminate()`는 실행 중인 작업을 중간에 끊고 큐에 남은 작업도 버리므로, 정상 종료
경로로 쓰지 않는다.

### 작업 실패

task 내부에서 예외가 발생하면 워커는 죽지 않는다. traceback을 로그로 남기고,
`(task, TaskExecutionError)` 로 결과 큐에 넣고, 다음 작업으로 넘어간다.
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
