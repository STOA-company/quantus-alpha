#!/usr/bin/env python3
"""gitleaks 보고(--redact)를 «파일:행 · 종류 · 커밋» 으로만 찍음 — 값·작성자·이메일은 안 찍음. usage: report.py <report.json>
워크플로 명령 값은 이스케이프(검수 r1 P2-2: 파일 이름의 줄바꿈·쉼표·콜론으로 명령 주입 막음). 꼴 틀린 보고도 죽지 않음."""
import json, sys


def esc(s, prop=False):
    s = str(s).replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    return s.replace(":", "%3A").replace(",", "%2C") if prop else s


try:
    items = json.load(open(sys.argv[1], encoding="utf-8"))
except (OSError, ValueError, IndexError):
    items = []
if not isinstance(items, list):
    items = []
seen = set()
for x in items:
    if not isinstance(x, dict):
        continue
    k = (str(x.get("File")), str(x.get("StartLine")), str(x.get("RuleID")))
    if k in seen:
        continue
    seen.add(k)
    print(f"::error file={esc(k[0], True)},line={esc(k[1], True)}::{esc('비밀 키 꼴 «' + k[2] + '» (커밋 ' + str(x.get('Commit') or '')[:8] + ')')}")
print(f"비밀 키 꼴 {len(seen)}곳")
