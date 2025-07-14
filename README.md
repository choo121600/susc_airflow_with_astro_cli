# SUSC Airflow 데이터 파이프라인 예제 프로젝트

## 프로젝트 구조
```
.
├── dags/
│   ├── exampledag.py                  # 전통적 방식의 DAG 예제
│   ├── exampledag2.py                 # TaskFlow API 기반 DAG 예제
│   ├── news_sentiment_analysis_dag.py # AI 뉴스 감정 분석 DAG
│   └── notification_example_dag.py    # 이메일 알림 예제 DAG
├── include/
├── plugins/
├── tests/
├── requirements.txt
├── airflow_settings.yaml
├── Dockerfile
├── packages.txt
└── README.md
```


## 주요 기능
* 전통적 DAG 예제: PythonOperator, BashOperator를 직접 사용한 명시적 의존성 관리
* TaskFlow API 예제: 최신 데코레이터 기반, 자동 XCom, 함수형 DAG 작성
* AI 뉴스 감정 분석:
    * Yahoo Finance RSS에서 실시간 뉴스 수집
    * OpenAI GPT-4o로 한글 요약 및 감정분석
    * 결과를 HTML 이메일로 자동 발송
* 이메일 알림 예제:
    * Airflow 최신 SMTP Notification 시스템
    * 태스크 성공/실패 시 이메일로 알림


## 환경 설정
### Airflow Variables & Connections
`airflow_settings.yaml`에서 다음 변수와 연결을 설정하세요.
```yaml
connections:
  - conn_id: smtp_default
    conn_type: smtp
    conn_host: smtp.gmail.com
    conn_login: your-email@gmail.com
    conn_password: your-app-password
    conn_port: 587
    conn_extra:
      disable_ssl: true
      disable_tls: false
      timeout: 30
      retry_limit: 5
variables:
  - variable_name: NOTIFICATION_FROM_EMAIL
    variable_value: your-email@gmail.com
  - variable_name: NOTIFICATION_TO_EMAIL
    variable_value: recipient@gmail.com
  - variable_name: OPENAI_API_KEY
    variable_value: sk-xxxx...
```

## 예제 DAG 설명

### 1. `exampledag.py`  
- **전통적 방식**
- PythonOperator, BashOperator 직접 사용
- 명시적 의존성 설정
- XCom을 통한 데이터 전달

### 2. `exampledag2.py`  
- **TaskFlow API 방식**
- `@dag`, `@task` 데코레이터
- 함수형 DAG, 자동 XCom
- 더 간결하고 Pythonic

### 3. `notification_example_dag.py`  
- **이메일 알림 전용 예제**
- Airflow 최신 SMTP Notification 시스템
- 태스크 성공/실패 시 이메일로 알림 전송

#### 이메일 알림 예시
- **제목**: `[Airflow 알림] notification_example_dag - send_email_task 실패`
- **본문**:  
  ```
  Airflow 태스크 실패 알림
  DAG ID: notification_example_dag
  Task ID: send_email_task
  실행 시간: 2025-07-14T09:00:00+00:00
  상태: 실패
  ```

### 4. `news_sentiment_analysis_dag.py`  
- **AI 기반 뉴스 감정 분석**
- 코드 상단의 `STOCK_SYMBOL` 변수로 분석 대상 주식 심볼 지정 (예: `"AAPL"`, `"TSLA"`, `"GOOGL"` 등)
- Yahoo Finance RSS에서 뉴스 수집
- OpenAI로 한글 요약 및 감정분석
- 결과를 HTML 이메일로 발송

#### 이메일 알림 예시
- **제목**: Daily News Sentiment Analysis Report - AAPL
- **본문**:  
  ```
  <html>
    <body>
      <h2>Daily News Sentiment Analysis Report - AAPL</h2>
      <p>AAPL 관련 뉴스 감정 분석 결과입니다.</p>
      <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; width: 100%;">
        <tr style="background-color: #f2f2f2;">
          <th>제목</th>
          <th>요약</th>
          <th>감정분석</th>
          <th>링크</th>
          ...
    </body>
  </html>
  ```


---

## 실행해보기

1. **Astro CLI 설치**  
   [공식 문서](https://docs.astronomer.io/astro/cli/install-cli) 참고

2. **프로젝트 시작**
   ```bash
   astro dev start
   ```

3. **Airflow UI 접속**  
   - http://localhost:8080  
   - 기본 계정: `admin` / `admin`

4. **DAG 실행**  
   - 원하는 DAG를 켜고 실행

5. **뉴스 심볼 변경**  
   - `dags/news_sentiment_analysis_dag.py` 상단의 `STOCK_SYMBOL` 값을 원하는 심볼로 변경 후 저장
