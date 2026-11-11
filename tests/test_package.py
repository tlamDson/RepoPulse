import re

import repopulse


def test_version_is_semver():
    assert re.fullmatch(r"\d+\.\d+\.\d+", repopulse.__version__)
