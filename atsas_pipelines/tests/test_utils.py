import pytest
from atsas_pipelines.utils import find_executable


@pytest.mark.parametrize('search_method', ['shutil', 'distutils'])
def test_find_executable_shutil(search_method):
    res = find_executable('ls', search_method=search_method)
    assert res is not None

    non_existent_executable = 'non-existent-executable123'
    with pytest.raises(RuntimeError):
        find_executable(non_existent_executable, search_method=search_method)
