from airflow.decorators import dag, task

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from datetime import timedelta


@dag(
    dag_id='exchange_rate_dag',
    schedule='@daily',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    catchup=False
)
def exchange_rate_dag():

    @task
    def get_usd_exchange_rate():
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')

        command_executor = "http://selenium-demo:4444/wd/hub"
        rate = None

        driver = webdriver.Remote(
            command_executor=command_executor,
            options=options
        )

        url = "https://finance.naver.com/marketindex/"

        try:
            driver.get(url)

            wait = WebDriverWait(driver, 10)
            xpath = "//*[@id='exchangeList']/li[1]/a/div/span[1]"

            exchange_usd_element = wait.until(
                EC.visibility_of_element_located((By.XPATH, xpath))
            )

            rate = exchange_usd_element.text

            print(f"USD Exchange Rate: {rate}")
        except Exception as e:
            print(f'USD Exchange Rate Error: {e}')
        finally:
            driver.quit()
        return rate

    get_usd_exchange_rate()


exchange_rate_dag()
