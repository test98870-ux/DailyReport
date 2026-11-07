# Android 2.1.2 실기기 검증 완료 (2026-11-08)

연결된 갤럭시 SM-S947N에 최종 2.1.2 (versionCode 6)를 업데이트 설치했습니다. 2.1.0 실기기 점검에서 강제 종료 시 WebView localStorage 쓰기 지연과 사진의 지연 로딩으로 인한 스크롤 이동을 발견해, Android SharedPreferences 즉시 저장과 사진 크기 복원으로 보완했습니다.

- `adb install -r`: 최종판 설치 성공
- 홈 이동 → 앱 아이콘 실행: 같은 기사·읽던 위치 복원, Activity/WebView 하나로 유지
- 앱 강제 종료 → 재실행: 같은 기사와 480px 읽던 위치 복원 (사진 1개 영역 포함)
- 지연 사진 로딩 브라우저 회귀 테스트: 통과
- 기존 모바일 UI 및 재실행/복귀 회귀 테스트: 통과
- 최종 `assembleDebug testDebugUnitTest lintDebug`: 성공
- APK 서명 검증: 통과
- 해당 앱 프로세스 AndroidRuntime/python.stderr/chromium 오류 로그: 오류 없음
- 실제 정리 기준: `2026-11-07T16:00:00+09:00`
- 재등록 제외 기록: 17건, 남은 자료 695건
- 기준 이전의 미보관 자료: 0건 (정리 적용 확인)
- Android 화면 상태 저장: 확인

보관함 보호·재등록 차단·정리 롤백은 기존 Python 테스트 52개에서 검증했습니다. 실기기에는 테스트 자료를 넣거나 정리 기록을 초기화하지 않았습니다. 직접적인 재수집 차단 테스트를 위해 실제 항목을 강제로 다시 삽입하는 작업은 하지 않았습니다. 세부 집계는 `artifacts/device-verification.json`에 있습니다.

최종 APK SHA-256: `2ad7f30952d8eadb2706e145edc75e704f6e326ea7ad33d571827c0d01a189cc`

---

# Android 2.1.0 검증 결과 (2026-11-08)

기존 수집 시작 시각과 자동 정리 기준을 일치시켰습니다. 기본 한국 시간 어제 16시 이전에 발행된 자료는 읽음 여부와 관계없이 정리하며, 보관함 자료와 정확히 경계 시각에 발행된 자료는 남깁니다. 3일/7일 설정에도 같은 기준이 적용됩니다.

- Python 테스트: **52개 통과**
- 경계 시각·읽음/안 읽음 정리·보관함 및 본문 보호·기간 확대/앱 재시작 후 재등록 방지: 통과
- 제목·소스·발행 시각 변경 및 알려진 뉴스/리서치 URL 별칭의 식별 기록 유지: 통과
- 같은 제목의 다른 URL을 별개로 처리: 통과
- 삭제 오류 시 목록·표시·본문·식별 기록 트랜잭션 롤백: 통과
- 이미 정리된 기사의 지연 본문 응답이 캐시를 재생성하지 않음: 통과
- `assembleDebug testDebugUnitTest lintDebug`: 성공 (단위 테스트 2개, Lint 오류 0개)
- 모바일 UI: 다섯 화면 크기와 기존 기사 복원 회귀 테스트 통과
- 자동 정리 기준·제외 건수·기간을 늘려도 복원되지 않는다는 설정 안내, 저장 후 목록 갱신: 통과
- APK v2 서명·16KB ZIP/ARM64 ELF 정렬·최종 assets 일치·PC 비공개 DB/.env 제외: 통과

버전 `2.1.0` (versionCode 4). 이 빌드에는 2.0.1의 기사·스크롤 복원과 단일 화면 실행 수정도 포함됩니다.

실기기 연결 확인 시 ADB 목록이 비어 있어 **2.1.0 휴대폰 업데이트 설치와 실기기 검증은 아직 하지 못했습니다**. 기존 2.0.1의 설치 성공 기록과 구분해야 합니다. 배포 APK는 `artifacts/daily-report-debug.apk`입니다.

SHA-256: `522c53e81da676472bdb0af529cf2222b74f59d61ab3e9caa58d772ea8cda7d3`

---

# Android 2.0.1 검증 결과 (2026-11-08)

홈 이동 후 앱을 다시 열 때 기사 화면이 초기화되던 문제를 수정했습니다. 앱 실행 화면을 `singleTask`로 재사용하고, 기사 ID·본문 및 목록 스크롤·검색어·카테고리·읽음 필터·글자 크기를 저장합니다. 새 WebView에서도 목록을 읽은 뒤 해당 기사를 복원합니다. 설정 입력 중인 인증키는 저장 상태에 넣지 않습니다.

- 브라우저 회귀 테스트: 기존 모바일 동작 및 기사 화면 reload/resume 후 기사·600px 스크롤·글자 크기 복원 통과
- 저장한 기사 ID를 찾을 수 없을 때 목록 복귀 및 자동 수집 없음 확인
- `assembleDebug testDebugUnitTest lintDebug`: 성공
- APK 서명 검사: 통과
- 연결된 갤럭시에 `adb install -r` 업데이트 설치: 성공 (기존 자료 삭제 없음)
- 실기기 홈 복귀·프로세스 재시작 회귀 테스트: USB 디버깅 연결이 없어 아직 실행 전

앱 버전은 `2.0.1` (versionCode 3)입니다. 최종 APK SHA-256은 `artifacts/daily-report-debug.apk.sha256`을 참고하세요.

---

# Android 2.0 검증 결과

- Python 수집기·모바일 백엔드 테스트: **43개 통과**
- Android 로컬 URL 접근 제한 단위 테스트: **2개 통과**
- Gradle assembleDebug / assembleDebugAndroidTest / testDebugUnitTest / lintDebug: **성공**
- Android Lint: 오류 0개, 경고 2개 (하위 API에서 무시되는 뒤로 가기 속성, 최신 Gradle 알림)
- 브라우저 UI: 320×740, 360×800, 412×915, 600×960, 896×412에서 가로 넘침 없음
- 목록 40개씩 추가 표시·검색·탭·본문 안전 표시·글자 크기·읽음·보관·설정·수동 수집 UI·결과·빈 상태 검증: 통과
- 실제 네이버 리서치: 기업·시황·산업·전략·경제 각 1건 이상 확인
- 실제 다음 뉴스 경제: 기사 2건 확인
- APK v2 서명 검증: 통과
- APK 16KB ZIP alignment 검사: 통과
- ARM64 네이티브 라이브러리 LOAD 구간 16KB ELF alignment 검사: 통과
- APK 화면 assets와 최종 소스 일치: 확인
- APK에 PC DB·.env·main.py가 포함되지 않음: 확인

## 아직 확인하지 못한 항목

Android 16 소프트웨어 에뮬레이터에서 앱 설치 전부터 system_server watchdog 종료와 PackageManager 재시작이 반복됐습니다. 설치 시 `device is still booting`, `Can't find service: package`, `Broken pipe` 오류로 중단되어 **Android 런타임 smoke test는 실행하지 못했습니다**. 이 결과는 앱의 Android 실동작 검증 통과를 의미하지 않습니다. 에뮬레이터는 종료했습니다.

연결된 실제 갤럭시가 없어 S26 플러스 설치·내장 Python 시작·기기 네트워크에서의 수집·WebView 표시·화면 전환 중 중단 동작은 실기기 검증이 남아 있습니다. 재현 방법은 README의 Android 런타임 smoke test 절차를 참고하세요.

## 설치 파일

- 경로: `artifacts/daily-report-debug.apk`
- 앱 ID: `kr.dailyreport.app`
- 버전: `2.0.0` (versionCode 2)
- 권한: INTERNET만 요청
- ABI: arm64-v8a / x86_64
- 서명: 개인 설치용 debug
- 크기: 36.7 MiB
- SHA-256: `eaeb5dd5a64ddf8db3906aecaa8fcb85a034527ad5ea8b07e066a160ba3f01cb`
