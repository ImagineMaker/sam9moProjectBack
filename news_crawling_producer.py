# 기본적인 문법 import
from time import sleep 
import json
from datetime import datetime
import re

#kafka producer 예제
from kafka import KafkaProducer

# Selenium Import
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import pymongo
from pymongo.errors import DuplicateKeyError

from multiprocessing import Process

# Selenium 세팅
# Headless Chrome 실행
op = Options()
op.add_argument(f"user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
op.add_argument('--headless')
op.add_argument("disable-gpu")

# mongodb 연결
client = pymongo.MongoClient('mongodb://sam9:sam9mo!!@59.3.28.12:27017/')
db = client["sam9mo"]
# mongdb colletion 세팅
collection = db["news"]

# kafa세팅
class MessageProducer:
    def __init__(self, broker, topic):
        self.broker = broker
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=self.broker,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            acks=0,
            api_version=(2, 5, 0),
            retries=3,
        )

    def send_message(self, msg, auto_close=False):
        try:
            future = self.producer.send(self.topic, msg)
            self.producer.flush()  # 비우는 작업
            if auto_close:
                self.producer.close()
            future.get(timeout=2)
            return {"status_code": 200, "error": None}
        except Exception as exc:
            raise exc

# 브로커와 토픽명을 지정한다.
brokers = ['59.3.28.12:9092', '59.3.28.12:9093', '59.3.28.12:9094']

STOCK_CODE_LIST = {
    '005930': '삼성전자',    '373220': 'LG에너지솔루션',    '000660': 'SK하이닉스',    '207940': '삼성바이오로직스',    '005935': '삼성전자우',
    '005490': 'POSCO홀딩스',    '005380': '현대차',    '051910': 'LG화학',    '035420': 'NAVER',    '000270': '기아',
    '006400': '삼성SDI',    '068270': '셀트리온',    '003670': '포스코퓨처엠',    '035720': '카카오',    '105560': 'KB금융',
    '028260': '삼성물산',    '012330': '현대모비스',    '055550': '신한지주',    '066570': 'LG전자',    '096770': 'SK이노베이션',
    '032830': '삼성생명',    '003550': 'LG',    '323410': '카카오뱅크',    '033780': 'KT&G',    '086790': '하나금융지주',
    '000810': '삼성화재',    '034730': 'SK',    '015760': '한국전력',    '138040': '메리츠금융지주',    '017670': 'SK텔레콤',
    '018260': '삼성에스디에스',    '011200': 'HMM',    '329180': 'HD현대중공업',    '010130': '고려아연',    '009150': '삼성전기',
    '047050': '포스코인터내셔널',    '259960': '크래프톤',    '316140': '우리금융지주',    '034020': '두산에너빌리티',    '024110': '기업은행',
    '247540': '에코프로비엠',    '086520': '에코프로',    '091990': '셀트리온헬스케어',    '022100': '포스코DX',    '066970': '엘앤에프',
    '028300': 'HLB',    '196170': '알테오젠',    '068760': '셀트리온제약',    '035900': 'JYP Ent.',    '277810': '레인보우로보틱스',    '403870': 'HPSP',
    '058470': '리노공업',    '263750': '펄어비스',    '214150': '클래시스',    '293490': '카카오게임즈',    '357780': '솔브레인',    '041510': '에스엠',
    '039030': '이오테크닉스',    '145020': '휴젤',    '005290': '동진쎄미켐',    '095340': 'ISC',    '112040': '위메이드',
    '240810': '원익IPS',    '036930': '주성엔지니어링',    '253450': '스튜디오드래곤',    '035760': 'CJ ENM',    '000250': '삼천당제약',
    '086900': '메디톡스',    '121600': '나노신소재',    '214370': '케어젠',    '067310': '하나마이크론',    '393890': '더블유씨피',
    '025900': '동화기업',    '034230': '파라다이스',    '237690': '에스티팜',    '078600': '대주전자재료',    '048410': '현대바이오',
    '141080': '레고켐바이오',    '365340': '성일하이텍',    '195940': 'HK이노엔'
}

s_keys = list(STOCK_CODE_LIST.keys())
s_values = list(STOCK_CODE_LIST.values())

PROCESS_COUNT = 2
chunk_size = len(s_keys) // PROCESS_COUNT

pr_news_info = MessageProducer(brokers, topic="send_news_content")
pr_func_status = MessageProducer(brokers, topic="send_function_status")

def send_function_status(*arg):
    if arg[2] == "Success":
        send_message = {
            "function_category" : arg[0],
            "function_name" :  arg[1],
            "function_status" : arg[2]
        }
    else:
        send_message = {
            "function_category" : arg[0],
            "function_name" :  arg[1],
            "function_status" : arg[2],
            "status_message" : arg[3],
            "problem_id" : arg[4],            
        }
    pr_func_status.send_message(send_message, False)
    
def crawling(p_index):
        
    # 뉴스 기사 수집할 종목코드 리스트
    STOCK_CODE_LIST_SLICE = {s_keys[i]: s_values[i] for i in range(p_index * chunk_size, (p_index+1) * chunk_size)}
    
    # 기사 본문에서 필요없는 특수문자 및 본문 양식 등을 다 지움    
    def clear_content(text):
        
        special_symbol = re.compile('[\{\}\[\]\/?,;:|\)*~`!^\-_+<>@\#$&▲▶◆◀■【】\\\=\(\'\"]')
        content_pattern = re.compile('본문 내용|TV플레이어| 동영상 뉴스|flash 오류를 우회하기 위한 함수 추가function  flash removeCallback|tt|앵커 멘트|xa0')
        
        newline_symbol_removed_text = text.replace('\n', '').replace('\t', '').replace('\r', '').replace("  ", "")
        special_symbol_removed_content = re.sub(special_symbol, ' ', newline_symbol_removed_text)
        end_phrase_removed_content = re.sub(content_pattern, '', special_symbol_removed_content)
        blank_removed_content = re.sub(' +', ' ', end_phrase_removed_content).lstrip()  # 공백 에러 삭제
        content = ''
        content = blank_removed_content
        return content

    # 종목코드, 페이지 번호에 따른 url설정
    def get_url(stock_code, idx):
        url = f'https://finance.naver.com/item/news.naver?code={stock_code}&sm=title_id.basic&page={idx}'
        return url

    # 브라우져 켜기
    driver = webdriver.Chrome(options=op)
    driver.maximize_window()

    CRAWLING_PAGE = 20
    
    while True:
        try:
            send_function_status(
                "news", 
                f'crawling_process{p_index}', 
                "Success",
            )

            for stock_code, stock_name in STOCK_CODE_LIST_SLICE.items():
                print(f'{stock_name} 수집 중 ....')
                
                # 해당 종목 크롤링 중단 토글
                break_toggle = False      
                
                SET_DUPLICATED_ID_COUNT = 5
                check_duplicated_id_count = 0
                crawling_count = 0
                get_news_count = 0

                for idx in range(1, CRAWLING_PAGE + 1):
                    
                    if break_toggle:
                        break_toggle = False
                        break
                    
                    url = get_url(stock_code, idx)
                    # 뉴스 기사 url로 이동
                    driver.get(url)
                    sleep(0.5)
                    # 페이지 내에서뉴스 기사가 있는 iframe으로 이동
                    driver.switch_to.frame('news_frame')
                    # 뉴스 기사 제목 요소 수집
                    news_title_element = driver.find_elements(By.CSS_SELECTOR, 'body > div > table.type5 > tbody > tr > td.title > a')
                    # 뉴스 기사 날짜 요소 수집
                    news_date_element = driver.find_elements(By.CSS_SELECTOR, 'body > div > table.type5 > tbody > tr > td.date')

                    for i in range(len(news_title_element)):
                        
                        news_date = news_date_element[i].text
                        date = datetime.strptime(news_date, "%Y.%m.%d %H:%M")
                        
                        news_year = date.year
                        news_month = date.month
                        news_day = date.day
                        news_hour = date.hour
                        news_minute = date.minute
                        
                        if len(str(date.month)) == 1:
                            news_month = "0" + str(date.month)
                        if len(str(date.day)) == 1:
                            news_day = "0" + str(date.day)
                        if len(str(date.hour)) == 1:
                            news_hour = "0" + str(date.hour)
                        if len(str(date.minute)) == 1:
                            news_minute = "0" + str(date.minute)
                        
                        ############ 크롤링 범위를 뉴스 기사 날짜로 설정 ####################
                        # compare_date = datetime.strptime(f'{news_year}{news_month}{news_day}{news_hour}{news_minute}', '%Y%m%d%H%M')

                        # start_date_str = ""
                        # if start_date_str == "":
                        #     start_date = datetime.strptime(datetime.now().strftime('%Y%m%d%H%M'), '%Y%m%d%H%M')
                        # else:
                        #     start_date = datetime.strptime(end_date_str, '%Y%m%d%H%M')
                            
                        # if compare_date > start_date:
                        #     print("패스")
                        #     driver.close()
                        #     sleep(0.5)
                        #     driver.switch_to.window(driver.window_handles[0])
                        #     driver.switch_to.frame('news_frame')
                        #     continue
                        
                        # end_date_str = "202311232000"
                        # end_date = datetime.strptime(end_date_str, '%Y%m%d%H%M')
                        # # 설정 년월이 되면 해당 종목 크롤링 중단
                        # if compare_date < end_date:
                        #     print("브레이크")
                        #     break_toggle = True
                        #     break
                        #########################################################################
                        
                        # 뉴스 기사 제목 텍스트 수집
                        news_title = news_title_element[i].text
                        # 뉴스 기사 링크 수집
                        news_link = news_title_element[i].get_attribute('href')
                        # i번째 뉴스 기사 접속
                        news_title_element[i].click()
                        sleep(0.5)
                        # 뉴스 기사가 열린 탭으로 이동
                        driver.switch_to.window(driver.window_handles[-1])
                        
                        try:
                            # 뉴스 기사 ID수집
                            news_id = driver.find_element(By.ID, '_SUMMARY_BUTTON').get_attribute("data-articleid")
                        except:
                            driver.close()
                            sleep(0.5)
                            driver.switch_to.window(driver.window_handles[0])
                            driver.switch_to.frame('news_frame')
                            crawling_count = crawling_count + 1
                            # print(f'{stock_name} 크롤링 진행 진행도 : {crawling_count}')
                            continue
                        
                        get_collection_id = collection.find_one({"news_id" : news_id})
                        
                        if get_collection_id != None:
                            check_duplicated_id_count = check_duplicated_id_count + 1
                            # print("중복된 기사 개수", check_duplicated_id_count)
                            if SET_DUPLICATED_ID_COUNT == check_duplicated_id_count:
                                break_toggle = True
                                driver.close()
                                sleep(0.5)
                                driver.switch_to.window(driver.window_handles[0])
                                driver.switch_to.frame('news_frame')
                                # print("이미 수집된 기사 다수 있음")
                                break
                            driver.close()
                            sleep(0.5)
                            driver.switch_to.window(driver.window_handles[0])
                            driver.switch_to.frame('news_frame')
                            crawling_count = crawling_count + 1
                            # print(f'{stock_name} 크롤링 진행 진행도 : {crawling_count}')
                            # print("해당 기사는 중복된 기사임")
                            continue
                        
                        # 뉴스 기사 본문 수집
                        element_content = driver.find_element(By.ID, 'newsct_article')
                        
                        try:
                            # 뉴스 기사 첫번째 이미지
                            element_img = driver.find_element(By.ID, 'img1')
                            news_img = element_img.get_attribute('src')
                        except Exception as ex:
                            news_img = ""
                            
                        # 뉴스 기사 본문 전처리(필요없는 문자 제거)
                        news_content = clear_content(element_content.text)
                        news_content_slice = ''
                        
                        if len(news_content) > 2000:
                            news_content_slice = clear_content(news_content)[:2000]
                        else:
                            news_content_slice = news_content
                            
                        # 언론사 
                        element_company = driver.find_element(By.XPATH, '//meta[@property="og:article:author"]')
                        news_company = element_company.get_attribute('content').split("|")[0].replace(" ", "")
                        # 카테고리
                        news_category = driver.find_element(By.CLASS_NAME, 'media_end_categorize_item').text
                        
                        current_time = datetime.now()
                        current_year = current_time.year
                        current_month = current_time.month
                        current_day = current_time.day
                        current_hour = current_time.hour
                        current_minute = current_time.minute
                        
                        if len(str(current_month)) == 1:
                            current_month = "0" + str(current_month)
                        if len(str(current_day)) == 1:
                            current_day = "0" + str(current_day)
                        if len(str(current_hour)) == 1:
                            current_hour = "0" + str(current_hour)
                        if len(str(current_minute)) == 1:
                            current_minute = "0" + str(current_minute)
                        
                        news_data = {
                            "_id": f'{news_year}{news_month}{news_day}{news_id}',
                            "news_year": f'{news_year}',
                            "news_month": f'{news_month}',
                            "news_day": f'{news_day}',
                            "news_id": news_id,
                            "news_company" : news_company,
                            "news_category": news_category,
                            "news_link": news_link,
                            "news_img": news_img,
                            "news_title": news_title,
                            "stock_company" : stock_name,
                            "news_time": f'{news_hour}{news_minute}',
                            "content_summary": "",
                            "keyword": "",
                            "news_sentiment": "",
                            "data_input_time": f'{current_year}{current_month}{current_day}{current_hour}{current_minute}',
                            "news_content" : news_content
                        }
                        
                        send_data = {
                            "_id": f'{news_year}{news_month}{news_day}{news_id}',
                            "news_content" : news_content_slice,
                            "data_input_time": f'{current_year}{current_month}{current_day}{current_hour}{current_minute}'
                        }
                        
                        try:
                            collection.insert_one(news_data)
                            get_news_count = get_news_count + 1
                        except Exception as ex:
                            send_function_status(
                                "news", 
                                f'crawling_process{p_index}', 
                                "Fail", 
                                f'but process continue.\n {ex}', 
                                f'{news_year}{news_month}{news_day}{news_id}'
                            )
                            driver.close()
                            sleep(0.5)
                            driver.switch_to.window(driver.window_handles[0])
                            driver.switch_to.frame('news_frame')
                            crawling_count = crawling_count + 1
                            # print(f'{stock_name} 크롤링 진행 진행도 : {crawling_count}')
                            continue
                        
                        pr_news_info.send_message(send_data,  False)
                        
                        # 뉴스 기사 탭 닫기
                        driver.close()
                        sleep(0.5)
                        # 뉴스 기사 리스트 탭으로 전환
                        driver.switch_to.window(driver.window_handles[0])
                        # 페이지 내에서뉴스 기사가 있는 iframe으로 이동
                        driver.switch_to.frame('news_frame')
                        crawling_count = crawling_count + 1
                        # print(f'{stock_name} 크롤링 진행 진행도 : {crawling_count}')
                    
                        
                # print("수집된 뉴스 기사 개수", get_news_count)     
                # print("크롤링 총횟수", crawling_count)       
                # print("해당 종목 리스트 크롤링 사이클 종료")
        except Exception as ex:
            send_function_status(
                "news", 
                f'crawling_process{p_index}', 
                "Fail", 
                f'process stop.\n {ex}'
            )
        
def start_crawling(p_index):
    crawling_process = Process(target=crawling, args=(p_index,))
    crawling_process.start()
    return crawling_process

if __name__ == "__main__":
    
    processes = []
    
    for p_index in range(PROCESS_COUNT):
        processes.append(start_crawling(p_index))
    
    # Wait for all processes to finish
    for process in processes:
        process.join()