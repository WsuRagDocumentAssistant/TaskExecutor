import logging
import queue
import traceback
from typing import Any

from multiprocessing import Process, Queue as create_queue
from multiprocessing.queues import Queue          # 타입 힌트용

from .exception import TaskExecutionError

logger = logging.getLogger(__name__)

# 큐에 이 값을 넣으면 워커가 루프를 빠져나온다.
# 워커 1개당 1개가 필요하다 (신호 하나는 워커 하나만 꺼내 간다).
SHUTDOWN = None


def _task_name(task) -> str:
    """로그와 에러 보고에 쓸 작업 이름."""
    return getattr(task, "__name__", repr(task))

# ------------------------
# Worker Process
# ------------------------

class TaskExecutor(Process):
    """작업 큐에서 꺼낸 작업을 실행하고 결과를 결과 큐로 보낸다.

    작업이 pickle 가능한지 보장하는 것은 작업을 만드는 쪽의 책임이다.
    """

    def __init__(self):
        super().__init__()
        self.task_queue = create_queue()
        # 생성은 팩토리 함수(multiprocessing.Queue)로 해야 한다.
        # 위의 multiprocessing.queues.Queue는 힌트 전용이며 ctx가 필수라
        # 직접 호출할 수 없다.
        self.result_queue = create_queue()

    def get_result_queue(self):
        return self.result_queue
    
    def get_task_queue(self):
            return self.task_queue

    def stop(self) -> None:
        """워커에게 종료를 요청한다.

        큐에 이미 쌓여있는 작업은 모두 처리한 뒤에 종료된다.
        호출 후 collect()로 결과를 비우고 join()으로 종료를 기다린다.
        """
        self.task_queue.put(SHUTDOWN)

    def get_task_result(self, timeout: float | None = None) -> Any:
        """결과를 하나 꺼낸다.

        timeout=None(기본)이면 결과가 올 때까지 무한 대기한다. 결과가
        올 예정이 없으면 영원히 멈추므로, 꺼낼 개수를 모를 때는 timeout을
        주거나 collect()를 쓴다.

        timeout을 주면 그 시간 안에 결과가 없을 때 queue.Empty가 발생한다.
        """
        return self.result_queue.get(timeout=timeout)

    def collect(self) -> list:
        """워커가 끝날 때까지 결과를 모두 꺼내 모은다.

        stop() 다음에 이것을 호출하고, 그 다음에 join()한다.
        결과 큐를 비우지 않으면 자식이 버퍼를 flush하지 못해 join()이 멈춘다.

        Empty만으로는 "더 없다"와 "아직 오는 중"이 구분되지 않으므로
        워커가 종료됐는지도 함께 확인한다.
        """
        items = []
        while True:
            try:
                items.append(self.result_queue.get(timeout=0.1))
            except queue.Empty:
                if not self.is_alive():   # 워커도 끝났고 큐도 비었다
                    return items

    def run(self) -> None:
        # spawn 방식에서는 자식이 새 인터프리터로 시작하므로
        # 로깅 설정이 상속되지 않는다. 여기서 직접 잡아준다.
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format="[%(processName)s] %(levelname)s %(message)s",
            )

        logger.info("Worker Start")

        while True:
            task = self.task_queue.get()   # 작업이 올 때까지 대기

            if task is SHUTDOWN:           # 종료 신호
                logger.info("Worker Stop")
                break

            try:
                result = task(task.params)
            except Exception:
                # 작업 하나가 실패해도 워커는 계속 살아있어야 한다.
                # run() 밖으로 예외를 던지면 워커가 죽고, 그 예외는
                # 부모에게 전달되지도 않는다. 실패는 결과 큐로 실어 보낸다.
                logger.exception("Task Failed")
                self.result_queue.put(
                    TaskExecutionError(
                        _task_name(task),
                        traceback.format_exc(),
                    )
                )
            else:
                logger.info("Task Finished")
                self.result_queue.put(result)
