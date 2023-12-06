# 기본적인 문법 import
from time import sleep 
import re

# Selenium Import
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import pymongo

# Selenium 세팅
# Headless Chrome 실행
op = Options()
op.add_argument(f"user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
op.add_argument('--headless')
op.add_argument("disable-gpu")

# mongodb 연결
client = pymongo.MongoClient('mongodb://sam9:sam9mo!!@59.3.28.12:27017/')
db = client["sam9mo"]
collection = db["news"]

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

def crawling():
    # 브라우져 켜기
    driver = webdriver.Chrome(options=op)
    driver.maximize_window()

    find_query = {"_id" : "202311290000951053"}
    # empty_news_content_list = list(collection.find({"news_content" : {"$exists" : False}}))
    empty_news_content_list = list(collection.find(find_query))
    
    crawling_count = 0
    
    for empty_document in empty_news_content_list:    
        # 뉴스 기사 url로 이동
        driver.get(empty_document['news_link'])
        sleep(0.5)
        print("current_link", empty_document['news_link'])
        # 뉴스 기사 본문 수집
        element_content = driver.find_element(By.ID, 'newsct_article')
        
        # 뉴스 기사 본문 전처리(필요없는 문자 제거)
        news_content = clear_content(element_content.text)

        news_data = {"news_content" : news_content}
        update_operation = {"$set": news_data}
        
        find_id_query = {"_id" : empty_document["_id"]}
        
        try:
            collection.update_one(find_id_query, update_operation)
            print("뉴스 기사 본문 저장 완료")
            crawling_count = crawling_count + 1
        except Exception as ex:
            print("뉴스 기사 요약 내용 삽입하는데 문제")
    driver.close()
    print("크롤링 총 횟수 ", crawling_count)

if __name__ == "__main__":
    crawling()