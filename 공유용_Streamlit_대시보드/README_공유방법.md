# AI 댐 관리 대시보드 공유 방법

## 1. 이 폴더의 목적

`공유용_Streamlit_대시보드`는 팀원이나 발표 환경에서 바로 실행할 수 있도록 만든 공유용 Streamlit 버전입니다.

이 버전은 `.env`, MySQL, 공공데이터포털 인증키 없이 실행됩니다.  
대신 로컬에서 수집하고 예측한 최신 결과를 CSV 스냅샷으로 저장해 `data` 폴더에 넣어 둔 구조입니다.

즉, 화면은 실제 대시보드와 비슷하게 재현하지만, 공유용 버전 자체가 30분마다 API를 새로 호출하지는 않습니다.

## 2. 실행 방법

터미널에서 이 폴더로 이동한 뒤 실행합니다.

```bash
cd 공유용_Streamlit_대시보드
pip install -r requirements.txt
streamlit run app.py
```

실행 후 브라우저에 표시되는 주소로 접속하면 됩니다. 보통 아래 주소입니다.

```text
http://localhost:8501
```

## 3. 포함된 주요 파일

| 파일 | 내용 |
|---|---|
| `app.py` | 공유용 Streamlit 대시보드 코드 |
| `requirements.txt` | 실행에 필요한 Python 패키지 |
| `data/latest_summary.csv` | 20개 댐 최신 수문값과 3시간 뒤 예측 결과 |
| `data/observation_history_72h.csv` | 최근 72시간 수문 운영 이력 |
| `data/weather_forecast_latest.csv` | 댐별 기상청 초단기/단기 예보 스냅샷 |
| `data/prediction_validation.csv` | 3시간 뒤 예측값과 실제값 검증 결과 |
| `data/downstream_representative.csv` | 대표 하류 수위관측소와 최근 수위/유량 |
| `data/downstream_candidates.csv` | 하류 수위관측소 후보 목록 |
| `data/five_day_range.csv` | 1일/3일/5일 누적 방류 구간 예측 |
| `data/release_horizon_summary.csv` | 3시간/6시간/1~5일 전체 실험 지표 |
| `data/release_horizon_by_dam.csv` | 댐별 3시간/6시간/1~5일 최고 모델 성능 |
| `data/interest_period_history.csv` | 공식 특보가 아닌 관측 강수·고유입 기준 과거 관심구간 일별 요약 |

## 4. 공유용 CSV 갱신

로컬 실험용 폴더에 최신 API 수집 및 ML 예측 결과가 생성된 뒤 아래 명령을 실행하면 공유용 CSV를 갱신할 수 있습니다.

```bash
python 갱신_공유용_데이터.py
```

이 스크립트는 최신 수문·예측 스냅샷과 실시간 검증 결과를 공유용 폴더로 반영하고, 과거 학습 데이터에서 `관측 강수`, `최근 24시간 누적강수`, `댐별 유입량 과거 상위 25%` 조건에 해당하는 관심구간 일별 요약을 생성합니다.

## 5. 실시간 API와의 차이

실시간 API 수집과 MySQL 저장은 로컬 실험용 프로젝트에서 수행합니다.

공유용 버전은 발표와 팀원 확인을 위해 CSV 파일만 읽습니다. 그래서 API 키를 GitHub에 올릴 필요가 없고, 팀원 컴퓨터에 MySQL이 없어도 대시보드를 볼 수 있습니다.

실시간 API까지 공유 버전에 붙이려면 다음 조건이 추가로 필요합니다.

- Streamlit Cloud의 `Secrets`에 공공데이터포털 인증키 저장
- 로컬 MySQL 대신 외부에서 접속 가능한 DB 사용
- API 호출 제한과 인증키 노출 방지 처리

현재 프로젝트 제출용으로는 CSV 공유 버전이 더 안전합니다.

## 6. GitHub 업로드 주의사항

- `.env` 파일은 절대 올리지 않습니다.
- MySQL 비밀번호와 공공데이터포털 인증키는 올리지 않습니다.
- 100MB가 넘는 학습용 CSV나 원본 대용량 CSV는 올리지 않습니다.
- 공유용 폴더의 `data` 파일들은 모두 작은 스냅샷이므로 GitHub 업로드가 가능합니다.

## 7. 발표에서 설명할 문장

> 공유용 대시보드는 실시간 API와 DB 없이도 동일한 화면을 재현할 수 있도록, 로컬에서 수집한 최신 수문 운영 정보, 기상예보, 하류 수위, 예측 결과를 CSV 스냅샷으로 저장한 버전입니다. 실제 운영 버전은 30분마다 API를 호출해 DB에 저장하고 예측을 갱신하지만, 공유와 발표 환경에서는 인증키 노출을 막기 위해 CSV 기반으로 시연합니다.
