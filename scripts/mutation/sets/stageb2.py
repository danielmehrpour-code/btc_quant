"""Mutationsmängd etapp B steg 2: verktyget för Binance-arkivet (scripts/data/fetch_binance.py, SB-S2-j).

Varje mutation tar bort eller kringgår en garanti i designen (DESIGN_step2.md avsnitt 9); testerna i
tests/test_fetch_binance.py ska döda alla 14.
"""
NAME = "stageb2"
TESTS = ("tests/test_fetch_binance.py",)
F = "scripts/data/fetch_binance.py"
MUTATIONS = {
    "1 checksum comparison removed": [
        (F, "            if sha != value:                                                   # 4\n",
         "            if False:\n"),
    ],
    "2 recheck writes the new version over the original": [
        (F, '        got = download(s, t, s.root / "revisions" / path / value, checksum=cbytes)\n',
         '        got = download(s, t, s.root / "objects" / path, checksum=cbytes)\n'),
        (F, ('    if os.path.lexists(target):\n'
             '        raise FileExistsError(f"målet finns redan: {target}")\n'),
         ('    if os.path.lexists(target):\n'
          '        subprocess.run(["chmod", "-R", "u+w", str(target)], check=True)\n'
          '        subprocess.run(["rm", "-rf", str(target)], check=True)\n')),
    ],
    "3 published files not made read-only": [
        (F, ("            for f in (cpath, stage / t.name):                                  # 5\n"
             "                os.chmod(f, 0o444)\n"), ""),
    ],
    "4 fetch downloads present targets again": [
        (F, "            if t.path in scan.originals:\n                continue\n", ""),
    ],
    "5 present objects not re-verified (sha256 trusted from CHECKSUM)": [
        (F, ('    sha, size = _sha256_file(d / name)\n'
             '    if sha != value:\n'
             '        return None, "sha256 skiljer sig från CHECKSUM", False\n'),
         "    sha, size = value, os.lstat(d / name).st_size\n"),
    ],
    "6 CHECKSUM file-name check removed": [
        (F, "    if m.group(2) != name:\n", "    if False:\n"),
    ],
    "7 host and scheme allowlist removed": [
        (F, ('        if p.scheme != self.scheme or p.netloc != self.netloc or p.query or p.fragment:\n'
             '            raise FetchError(url, "refused_url")\n'), ""),
    ],
    "8 redirects followed": [
        (F, "urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())",
         "urllib.request.build_opener(urllib.request.ProxyHandler({}))"),
    ],
    "9 size limit removed": [
        (F, '            if size > limit:\n                raise FetchError(url, "too_large")\n', ""),
    ],
    "10 tmp cleanup removes unknown entries": [
        (F, ('            if ok:\n'
             '                scan.stages.append(entry)\n'
             '                scan.add("TMP_LEFTOVER", f"tmp/{entry}")\n'
             '            else:\n'
             '                scan.add("TMP_UNKNOWN", f"tmp/{entry}")\n'),
         ('            scan.stages.append(entry)\n'
          '            scan.add("TMP_LEFTOVER", f"tmp/{entry}")\n')),
    ],
    "11 root without marker accepted": [
        (F, ('    if not os.path.lexists(r / "ARCHIVE.json"):\n'
             '        raise RefusedError("befintlig katalog utan ARCHIVE.json; används aldrig")\n'), ""),
    ],
    "12 timestamp written into the inventory": [
        (F, '    doc = {"format": INVENTORY_FORMAT,\n',
         '    doc = {"format": INVENTORY_FORMAT, "written_at": datetime.now(UTC).isoformat(),\n'),
    ],
    "13 writer lock not taken": [
        (F, "            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)\n", "            pass\n"),
    ],
    "14 object published before the checksum comparison": [
        (F, ("            sha, size = s.stream(t.url, stage / t.name, MAX_DATA_BYTES)        # 3\n"
             "            if sha != value:                                                   # 4\n"),
         ("            sha, size = s.stream(t.url, stage / t.name, MAX_DATA_BYTES)        # 3\n"
          "            _publish_dir(s.root, stage, target_dir)\n"
          "            if sha != value:                                                   # 4\n")),
    ],
}
