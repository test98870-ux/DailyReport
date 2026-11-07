#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
apk_path="${1:-$repo_dir/artifacts/daily-report-debug.apk}"
if ! command -v adb >/dev/null 2>&1; then
    echo 'Android SDK Platform Tools의 adb를 설치하고 PATH에 추가하세요.' >&2
    exit 1
fi
if [[ ! -f "$apk_path" ]]; then
    echo "APK가 없습니다: $apk_path" >&2
    echo 'android/README.md의 빌드 절차를 먼저 실행하세요.' >&2
    exit 1
fi
adb_args=()
if [[ -n "${ANDROID_SERIAL:-}" ]]; then
    adb_args=(-s "$ANDROID_SERIAL")
fi
if ! adb "${adb_args[@]}" get-state >/dev/null 2>&1; then
    adb devices -l
    echo '휴대폰 USB 디버깅 연결과 PC 허용 창을 확인하세요. 여러 기기 연결 시 ANDROID_SERIAL을 지정하세요.' >&2
    exit 1
fi
adb "${adb_args[@]}" install -r "$apk_path"
adb "${adb_args[@]}" shell am start -n kr.dailyreport.app/.MainActivity
echo '설치 완료. 서버 연결 없이 앱의 지금 수집 버튼을 눌러 시작하세요.'
