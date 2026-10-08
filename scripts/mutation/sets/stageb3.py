"""Mutationsmängd etapp B steg 3a: schemat för datauppdelningen (DataSplitConfig, D-B-h svar 1 och 2).

Varje mutation tar bort eller försvagar en regel: halvöppna gränser på open_time, ordning, överlappning,
minsta lucka, alla tre eller ingen, UTC och strikt tolkning av YAML.
"""
NAME = "stageb3"
TESTS = ("tests/test_data_split.py", "tests/test_review_regressions.py", "tests/test_config.py")
F = "btc_trading_system/config/settings.py"
MUTATIONS = {
    "1 gap check removed": [
        (F, "            if b.start - a.end < MIN_SPLIT_GAP:\n", "            if False:\n"),
    ],
    "2 gap of exactly seven days refused (off by one)": [
        (F, "            if b.start - a.end < MIN_SPLIT_GAP:\n", "            if b.start - a.end <= MIN_SPLIT_GAP:\n"),
    ],
    "3 order and overlap check removed": [
        (F, "            if b.start < a.end:\n", "            if False:\n"),
    ],
    "4 start bound exclusive": [
        (F, "        return self.start <= open_time < self.end\n", "        return self.start < open_time < self.end\n"),
    ],
    "5 end bound inclusive": [
        (F, "        return self.start <= open_time < self.end\n", "        return self.start <= open_time <= self.end\n"),
    ],
    "6 all three or none not enforced": [
        (F, "        if any(s is None for s in segs) and any(s is not None for s in segs):\n", "        if False:\n"),
    ],
    "7 start before end not enforced": [
        (F, "        if not self.start < self.end:\n", "        if False:\n"),
    ],
    "8 bounds not required to be UTC": [
        (F, '        require_utc(self.start, "data_split.start")\n        require_utc(self.end, "data_split.end")\n', ""),
    ],
    "9 segment_of accepts non-UTC times": [
        (F, '        require_utc(open_time, "open_time")\n', ""),
    ],
    "10 minimum gap six days": [
        (F, "MIN_SPLIT_GAP = timedelta(days=7)", "MIN_SPLIT_GAP = timedelta(days=6)"),
    ],
    "11 unknown keys in data_split accepted": [
        (F, "    if set(ds) != set(SPLIT_SEGMENTS):\n", "    if not set(SPLIT_SEGMENTS) <= set(ds):\n"),
    ],
    "12 segment keys not checked": [
        (F, '        if not isinstance(v, dict) or set(v) != {"start", "end"}:\n', "        if not isinstance(v, dict):\n"),
    ],
}
