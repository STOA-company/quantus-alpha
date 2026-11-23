#!/bin/bash
# secret-scan 관문 본체 — 새로 들어오는 커밋에 비밀 키 꼴이 있으면 rc 1. 값은 어디에도 안 찍음(--redact · report.py 는 파일:행·종류만).
# 대표 결정 CX1123-08 = B(D26556). 정본: mwork2 ~/runs/armada/secgate-1123/repo (주인·Armada).
# 입력(env): EV(pull_request|push) · PR_HEAD · BASE_REF(PR 대상 가지 이름) · PR_BASE(옛 base sha, 대상 가지 ref 가 없을 때만) · BEFORE · AFTER · REF_NAME(push 가지 이름)
#   GL(gitleaks 실행 파일) · CFG(설정, 기본 .github/secret-scan/gitleaks.toml)
# 범위(검수 r1 P1-1·P1-2):
#   PR   = origin/<BASE_REF>..PR_HEAD(대상 가지의 모든 조상을 뺌 — 옛 base sha·교차 머지·rebase 에도 옛 키 다시 안 봄)
#   push = BEFORE..AFTER · BEFORE 가 없거나(새 가지) 못 가져오면(force-push) «AFTER 에서 다른 원격 가지들에 없는 커밋 전부»
# 실패 닫기(P1-3): 범위를 git 으로 먼저 검증 · gitleaks 가 ERR/FTL 를 내거나 보고 파일이 없으면 실패.
# 알려진 구멍(범위 줄임 — 검수 r2 qeamn4 뒤): 머지 커밋이 충돌을 풀며 «양쪽 부모 어디에도 없던 줄»로 넣은 키는 안 봄
#   (gitleaks git 은 머지 커밋 diff 를 안 읽음 · --cc 줄 뽑기는 경로·부모 수에서 오탐·구멍이 나 뺌). 보통 커밋으로 넣은 키는 다 봄.
# .gitleaksignore(검수 r1 P2-3): 검사 전 작업 트리에서 지움(파일·폴더) — PR 이 무시 목록을 넣어 피해 가지 못하게. r2 P2-2 의 --gitleaks-ignore-path 빈 폴더는 8.30.1 에서 ./.gitleaksignore 를 못 끔(실측).
# gitleaks 오류 판정(r2 P1-1): --no-color + 색 코드 지우고 ERR/FTL 찾음.
set -uo pipefail
CFG="${CFG:-.github/secret-scan/gitleaks.toml}"
d="$(mktemp -d)"; trap 'rm -rf "$d"' EXIT
fail() { echo "secret-scan 실패: $*"; exit 1; }
[ -x "${GL:-}" ] || fail "gitleaks 실행 파일 없음"
git rev-parse --git-dir >/dev/null 2>&1 || fail "git 저장소 아님"
sha_ok() { [ -n "$1" ] && git cat-file -e "$1^{commit}" 2>/dev/null; }

if [ "${EV:-}" = pull_request ]; then
  sha_ok "${PR_HEAD:-}" || fail "PR 머리 커밋을 못 찾음"
  base=""
  if [ -n "${BASE_REF:-}" ] && sha_ok "refs/remotes/origin/${BASE_REF}"; then base="refs/remotes/origin/${BASE_REF}"
  elif sha_ok "${PR_BASE:-}"; then base="${PR_BASE}"
  else fail "PR 대상 가지를 못 찾음"; fi
  set -- "${PR_HEAD}" --not "$base"
else
  sha_ok "${AFTER:-}" || fail "push 커밋을 못 찾음"
  if [ -n "${BEFORE:-}" ] && [ "${BEFORE}" != 0000000000000000000000000000000000000000 ] && ! sha_ok "${BEFORE}"; then
    git fetch -q --no-tags origin "${BEFORE}" >/dev/null 2>&1 || true
  fi
  if sha_ok "${BEFORE:-}"; then
    set -- "${AFTER}" --not "${BEFORE}"
  else
    others=()
    while read -r ref; do
      [ "$ref" = "refs/remotes/origin/${REF_NAME:-}" ] && continue
      [ "$ref" = "refs/remotes/origin/HEAD" ] && continue
      others+=("$ref")
    done < <(git for-each-ref --format='%(refname)' refs/remotes/origin/)
    if [ ${#others[@]} -gt 0 ]; then set -- "${AFTER}" --not "${others[@]}"; else set -- "${AFTER}"; fi
  fi
fi
n="$(git rev-list --count "$@" 2>/dev/null)" || fail "검사 범위 계산 실패"
echo "검사 범위: 새 커밋 ${n}개 ($*)"
[ "$n" -gt 0 ] || { echo "비밀 키 꼴 0 (새 커밋 없음)"; exit 0; }
rm -rf -- .gitleaksignore   # CI 작업 트리는 쓰고 버림 — --gitleaks-ignore-path 로는 ./.gitleaksignore 가 그대로 적용됨(11-23 실측)

found=0
"$GL" git . --config "$CFG" --redact --no-banner --no-color --log-level warn --log-opts="$*" \
  --report-format json --report-path "$d/r.json" --exit-code 3 >/dev/null 2>"$d/gl.err"; rc=$?
sed -E 's/\x1b\[[0-9;]*m//g' "$d/gl.err" > "$d/gl.txt"
if [ "$rc" = 3 ]; then found=1
elif [ "$rc" != 0 ] || [ ! -s "$d/r.json" ] || grep -qE '(^|[[:space:]])(ERR|FTL)([[:space:]]|$)' "$d/gl.txt"; then
  fail "gitleaks 실행 오류(rc=${rc}) — 통과로 치지 않음"
fi
[ "$found" = 1 ] && python3 .github/secret-scan/report.py "$d/r.json"

m="$(git rev-list --count --merges "$@" 2>/dev/null || echo 0)"

if [ "$found" = 1 ]; then
  echo "비밀 키 꼴 있음 — 값은 금고(Key Vault)·환경 변수로 옮기고 커밋을 고쳐 다시 올리세요. 시험용 가짜 값이면 그 줄 끝에 «gitleaks:allow» 주석."
  exit 1
fi
echo "비밀 키 꼴 0 (새 커밋 ${n}개 · 그중 머지 커밋 ${m}개는 충돌 해결 줄 안 봄)"
