import os


def find_executable(exec_name, search_method='shutil'):
    """
    Find an executable to perform the calculations with.

    Parameters
    ----------
    exec_name : str
        the name of the executable to search for
    search_method : str
        the search method of the executable.
        Supported methods: ``distutils`` or ``shutil``. Default is ``shutil``
        as it properly identifies the binaries with the executable bits.
    """
    supported_methods = ('shutil', 'distutils', )
    if search_method not in supported_methods:
        raise RuntimeError(f'The search method "{search_method}" '
                           f'is not in the list of supported methods: '
                           f'{supported_methods}')

    if search_method == 'shutil':
        import shutil
        exec_path = shutil.which(exec_name)
    elif search_method == 'distutils':
        import distutils.spawn
        exec_path = distutils.spawn.find_executable(executable=exec_name)

    if exec_path is None:
        raise RuntimeError(f'Executable "{exec_name}" was not found')

    if not os.path.isfile(exec_path):
        raise RuntimeError(f'Found executable {exec_path} is not a file')

    return exec_path
