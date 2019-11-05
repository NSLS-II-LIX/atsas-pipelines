from .utils import find_executable


def run_calc(exec_name='dammif', *args, **kwargs):
    exec_path = find_executable(exec_name)
    print(exec_path)
