


class TaskExecutionError(Exception):
    """워커에서 task 실행이 실패했음을 부모에게 전달한다.

    원본 예외 객체가 아니라 작업 이름과 traceback을 문자열로만 담는다.
    원본 예외는 그 자체가 pickle 불가능할 수 있어서, 실패를 알리려던
    통로가 다시 실패하게 된다.
    """

    def __init__(self, task_name: str, tb: str):
        super().__init__(task_name, tb)   # pickle 재구성용: 인자를 전부 넘긴다
        self.task_name = task_name
        self.tb = tb

    def __str__(self) -> str:
        return f"{self.task_name} failed:\n{self.tb}"
