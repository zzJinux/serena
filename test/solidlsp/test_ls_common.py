import os
import time

import pytest

from solidlsp import SolidLanguageServer
from solidlsp.ls_config import Language
from test.conftest import is_ci, is_windows


class TestLanguageServerCommonFunctionality:
    """Test common functionality of SolidLanguageServer base implementation (not language-specific behaviour)."""

    @pytest.mark.skipif(
        is_ci and is_windows, reason="This test is flaky in Windows CI (file system does not update modified time reliably)."
    )
    @pytest.mark.parametrize("language_server", [Language.PYTHON], indirect=True)
    def test_open_file_resync_on_reopen(self, language_server: SolidLanguageServer) -> None:
        file_path = os.path.join(language_server.repository_root_path, "test_open_file.py")
        rel_path = os.path.relpath(file_path, language_server.repository_root_path)
        test_string1 = "# foo"
        test_string2 = "# bar"
        with open(file_path, "w") as f:
            f.write(test_string1)
        try:
            with language_server.open_file(rel_path) as fb_outer:
                assert fb_outer.contents == test_string1
                # External modification while buffer is open
                time.sleep(1)
                with open(file_path, "w") as f:
                    f.write(test_string2)
                # Plain attribute — does NOT auto-detect
                assert fb_outer.contents == test_string1
                # Re-open triggers refresh on the existing buffer
                with language_server.open_file(rel_path) as fb_inner:
                    assert fb_inner is fb_outer
                    assert fb_inner.contents == test_string2
        finally:
            os.remove(file_path)
