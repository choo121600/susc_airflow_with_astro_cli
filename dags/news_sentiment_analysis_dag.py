from airflow import Dataset
from airflow.decorators import dag, task
from airflow.sdk import Variable
from airflow.providers.smtp.notifications.smtp import send_smtp_notification
from pendulum import datetime
from openai import OpenAI
import feedparser
import os

# 분석할 주식 심볼 설정
STOCK_SYMBOL = "AAPL"  # AAPL, GOOGL, MSFT, TSLA, AMZN, NVDA 등

def get_openai_api_key():
    """OpenAI API 키를 가져옵니다."""
    try:
        return Variable.get('OPENAI_API_KEY')
    except:
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("OPENAI_API_KEY not found in Variables or environment")
        return api_key

def get_smtp_notification():
    """SMTP 알림 콜백 함수를 반환합니다."""
    from_email = Variable.get('NOTIFICATION_FROM_EMAIL')
    to_email = Variable.get('NOTIFICATION_TO_EMAIL')
    
    return send_smtp_notification(
        from_email=from_email,
        to=to_email,
        smtp_conn_id="smtp_default",
        subject="[Airflow 알림] {{ dag.dag_id }} - {{ ti.task_id }} 실패",
        html_content="""
        <h3>Airflow 태스크 실패 알림</h3>
        <p><strong>DAG ID:</strong> {{ dag.dag_id }}</p>
        <p><strong>Task ID:</strong> {{ ti.task_id }}</p>
        <p><strong>실행 시간:</strong> {{ ts }}</p>
        <p><strong>상태:</strong> 실패</p>
        """
    )

def get_openai_response(prompt, model="gpt-4o"):
    """OpenAI API를 호출하여 응답을 받습니다."""
    try:
        client = OpenAI(api_key=get_openai_api_key())
        response = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system", 
                    "content": "You are a professional financial news analyst, highly experienced in summarizing stock market trends and financial news concisely."
                },
                {"role": "user", "content": prompt}
            ]
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"Error calling OpenAI API: {e}")
        return None

def fetch_news_from_rss(symbol):
    """RSS feed에서 주식 뉴스를 가져옵니다."""
    try:
        rss_urls = [
            f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US",
            f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}",
        ]
        
        all_articles = []
        for url in rss_urls:
            try:
                feed = feedparser.parse(url)
                if feed.entries:
                    for entry in feed.entries[:10]:
                        all_articles.append({
                            "title": entry.title,
                            "link": entry.link,
                            "relatedTickers": [symbol]
                        })
                    break
            except Exception as e:
                continue
                
        return all_articles[:10] if all_articles else []
    except Exception as e:
        return []

def get_fallback_news(symbol):
    """Fallback 뉴스 데이터를 생성합니다."""
    # 심볼에 따른 회사명 매핑
    company_names = {
        'AAPL': 'Apple',
        'GOOGL': 'Google',
        'MSFT': 'Microsoft',
        'TSLA': 'Tesla',
        'AMZN': 'Amazon',
        'NVDA': 'NVIDIA'
    }
    
    company_name = company_names.get(symbol, symbol)
    
    return [
        {
            "title": f"{company_name}'s Innovation in AI and Machine Learning Continues",
            "link": f"https://finance.yahoo.com/quote/{symbol}",
            "relatedTickers": [symbol]
        },
        {
            "title": f"{company_name} Stock Shows Strong Market Performance",
            "link": f"https://finance.yahoo.com/quote/{symbol}/news",
            "relatedTickers": [symbol]
        },
        {
            "title": f"{company_name} Services Revenue Reaches New Heights",
            "link": f"https://finance.yahoo.com/quote/{symbol}/financials",
            "relatedTickers": [symbol]
        },
        {
            "title": f"{company_name} Exceeds Market Expectations This Quarter",
            "link": f"https://finance.yahoo.com/quote/{symbol}/analysis",
            "relatedTickers": [symbol]
        },
        {
            "title": f"{company_name}'s Sustainability Initiatives Drive ESG Investment",
            "link": f"https://finance.yahoo.com/quote/{symbol}/sustainability",
            "relatedTickers": [symbol]
        },
    ]

default_args = {
    "owner": "Yeonguk",
    "retries": 1,
    "on_failure_callback": [get_smtp_notification()],
    "on_retry_callback": [get_smtp_notification()],
}

@dag(
    dag_id="news_sentiment_analysis",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=f"""
    ## 뉴스 감정 분석 DAG
    
    이 DAG는 다음과 같은 작업을 수행합니다:
    1. {STOCK_SYMBOL} 주식 관련 뉴스 추출 (RSS feed 사용)
    2. OpenAI API를 사용하여 뉴스 요약
    3. 감정 분석 수행
    4. 결과를 이메일로 전송
    
    ### 현재 분석 대상:
    - **주식 심볼**: {STOCK_SYMBOL}
    - **변경 방법**: 코드 상단의 STOCK_SYMBOL 변수 수정
    
    ### 뉴스 소스:
    1. Yahoo Finance RSS feed
    2. Fallback 샘플 데이터
    
    ### 필요한 Variables:
    - `OPENAI_API_KEY`: OpenAI API 키
    - `NOTIFICATION_FROM_EMAIL`: 발신자 이메일
    - `NOTIFICATION_TO_EMAIL`: 수신자 이메일
    """,
    default_args=default_args,
    tags=["example", "openai", "sentiment-analysis", "news", STOCK_SYMBOL.lower()],
)
def news_sentiment_analysis():
    """뉴스 감정 분석 DAG"""

    @task
    def extract_news(**context):
        """설정된 주식 심볼 관련 뉴스를 RSS feed에서 추출합니다."""
        
        print(f"분석 대상 주식: {STOCK_SYMBOL}")
        
        # RSS feed에서 뉴스 가져오기
        print("RSS feed에서 뉴스 추출 중...")
        news_data = fetch_news_from_rss(STOCK_SYMBOL)
        
        if news_data:
            print(f"RSS feed 성공: {len(news_data)}개 기사")
        else:
            # Fallback 데이터
            print("RSS feed 실패, fallback 데이터 사용")
            news_data = get_fallback_news(STOCK_SYMBOL)

        print(f"최종 추출된 뉴스: {len(news_data)}개")
        return news_data

    @task
    def summarize_news(extracted_news):
        """뉴스 기사를 요약합니다."""
        summarized_news = []
        for article in extracted_news:
            title = article.get('title', '')
            prompt = f"Please summarize this news article in one sentence in Korean: {title}"
            summary = get_openai_response(prompt)
            
            article_with_summary = article.copy()
            article_with_summary["summary"] = summary if summary else "Summary not available."
            summarized_news.append(article_with_summary)
            
        return summarized_news

    @task
    def evaluate_news(summarized_news):
        """뉴스의 감정을 분석합니다."""
        evaluated_news = []
        for article in summarized_news:
            title = article.get('title', '')
            summary = article.get('summary', '')
            
            prompt = (
                f"Evaluate the sentiment of the following article and provide a score (-10 to 10) and brief reasoning in Korean:\n"
                f"Title: {title}\nSummary: {summary}"
            )
            sentiment = get_openai_response(prompt)
            
            article_with_sentiment = article.copy()
            article_with_sentiment["sentiment"] = sentiment if sentiment else "Sentiment not available."
            evaluated_news.append(article_with_sentiment)
            
        return evaluated_news

    @task
    def send_news_email(evaluated_news, **context):
        """분석 결과를 이메일로 전송합니다."""
        try:
            from_email = Variable.get('NOTIFICATION_FROM_EMAIL')
            to_email = Variable.get('NOTIFICATION_TO_EMAIL')
            
            content = f"""
            <html>
                <body>
                    <h2>Daily News Sentiment Analysis Report - {STOCK_SYMBOL}</h2>
                    <p>{STOCK_SYMBOL} 관련 뉴스 감정 분석 결과입니다.</p>
                    <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; width: 100%;">
                        <tr style="background-color: #f2f2f2;">
                            <th>제목</th>
                            <th>요약</th>
                            <th>감정분석</th>
                            <th>링크</th>
                        </tr>
            """
            
            for article in evaluated_news:
                title = article.get('title', 'N/A')
                summary = article.get('summary', 'N/A')
                sentiment = article.get('sentiment', 'N/A')
                link = article.get('link', '#')
                
                content += f"""
                    <tr>
                        <td style="max-width: 300px;">{title}</td>
                        <td style="max-width: 400px;">{summary}</td>
                        <td style="max-width: 300px;">{sentiment}</td>
                        <td><a href="{link}" target="_blank">Read more</a></td>
                    </tr>
                """
                
            content += """
                    </table>
                    <br>
                    <p><em>Generated by Airflow News Sentiment Analysis DAG</em></p>
                </body>
            </html>
            """
            
            notification = send_smtp_notification(
                from_email=from_email,
                to=to_email,
                smtp_conn_id="smtp_default",
                subject=f"Daily News Sentiment Analysis Report - {STOCK_SYMBOL}",
                html_content=content
            )
            
            notification(context)
            
        except Exception as e:
            print(f"이메일 전송 중 에러: {e}")
            raise

    # Task dependencies 설정
    extracted_news = extract_news()
    summarized_news = summarize_news(extracted_news)
    evaluated_news = evaluate_news(summarized_news)
    send_news_email(evaluated_news)


news_sentiment_analysis()
