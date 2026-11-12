# 생활권 아파트 지도 (zipzip_spot)

초품아 · 스세권 · 맥세권 · 백세권 · 다세권 · 올세권 기준으로 아파트를 찾고, 준공연도·세대수로 거르는 지도 웹앱입니다.

- `index.html` : 앱 본체
- `config.js` : 카카오 JavaScript 키 (`window.KAKAO_APP_KEY`)
- `scripts/collect_apts.py` : 공공데이터포털 K-apt API로 단지 기본정보(준공일·세대수·동수) 수집
- `.github/workflows/collect-apts.yml` : 매일 새벽 자동 수집 → `data/apts.json` 저장
- 필요한 Secret : `DATA_GO_KR_KEY` (공공데이터포털 일반 인증키)

데이터 출처: 카카오 로컬, 국토교통부 공동주택관리정보시스템(K-apt). 거리는 직선거리 기준입니다.
