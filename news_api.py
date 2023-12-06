from fastapi import FastAPI, HTTPException, Path
from fastapi.middleware.cors import CORSMiddleware

from pydantic import BaseModel, ValidationError, Field
from enum import Enum

import pymongo

import uvicorn

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

NEWS_CATEGORY = {"politics" : "정치", "money" : "경제", "society" : "사회"}

client = pymongo.MongoClient('mongodb://sam9:sam9mo!!@59.3.28.12:27017/')
db = client["sam9mo"]
collection = db["news"]

def onStartUp():
    print('server start')
    
def onShutDown():
    print('server disconnected')
    
app = FastAPI(on_startup=[onStartUp], on_shutdown=[onShutDown])

class News_data(BaseModel):
    id: str = Field(title = "Mongdb 뉴스 데이터 ID", alias="_id")
    news_year: str = Field(title = "뉴스 기사 작성 년도")
    news_month: str = Field(title = "뉴스 기사 작성 월")
    news_day: str = Field(title = "뉴스 기사 작성 일")
    news_id: str = Field(title = "뉴스 기사 ID")
    news_company : str = Field(title = "뉴스 기사 작성 언론사")
    news_category: str = Field(title = "뉴스 기사 카테고리")
    news_link: str = Field(title = "뉴스 기사 링크")
    news_img: str = Field(title = "뉴스 기사 이미지")
    news_title: str = Field(title = "뉴스 기사 제목")
    stock_company: str = Field(title = "뉴스 기사 관련 종목")
    news_time: str = Field(title = "뉴스 기사 입력 시간")
    content_summary: str = Field(title = "뉴스 기사 내용 요약")
    keyword: list = Field(title = "뉴스 기사 키워드 리스트")
    news_sentiment: str = Field(title = "뉴스 기사 감성 분석")
    data_input_time: str = Field(title = "크롤링된 시간")
    
class News_List(BaseModel):
    news_list: list[News_data] = Field(title="요청한 페이지의 뉴스 리스트(15개)")    
    
class Keyword_Data(BaseModel):
    keyword_count_list: list[dict] = Field(title="요청한 날짜의 전체 키워드별 개수")
    
class Sentiment_Data(BaseModel):
    stock_sentiment_count: dict = Field(title="요청한 날짜의 특정 종목의 감성분석별 요소 개수")
    
class Bubble_Chart_Data(BaseModel):
    bubble_chart_data_list: list[dict] = Field(title="요청한 날짜의 특정 종목의 감성분석별 요소 개수")
    
class Catagory_List(str, Enum):
    politics = "politics"
    money = "money"
    society = "society"

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def date_parser(date):
    date_year = date[:4]
    date_month = date[4:6]
    date_day = date[6:]

    if len(str(date_month)) == 1:
        date_month = "0" + str(date_month)
    if len(str(date_day)) == 1:
        date_day = "0" + str(date_day)
    
    result = {
        "year" : date_year,
        "month" : date_month,
        "day" : date_day
    }
    
    return result

NEWS_COUNT_IN_PAGE = 15

def get_news_whole_list(page_Idx: int):
    
    find_query = {
        "cotent_summary" : {"$ne" : ""}, 
        "keyword" : {"$ne" : []}, 
        "news_sentiment" : {"$ne" : ""}
    }
    
    send_data = {
        "news_list" : list(collection
                           .find(find_query)
                           .skip(NEWS_COUNT_IN_PAGE * (page_Idx - 1))
                           .limit(NEWS_COUNT_IN_PAGE * page_Idx)
                           .sort("data_input_time", -1))
    }
    
    return send_data

def get_news_stock_list(page_Idx: int, stock_code: str):
    
    find_query = {
        "stock_company" : STOCK_CODE_LIST[stock_code], 
        "cotent_summary" : {"$ne" : ""}, 
        "keyword" : {"$ne" : ""}, 
        "news_sentiment" : {"$ne" : ""}
    }
    send_data = {
        "news_list" : list(collection
                           .find(find_query)
                           .sort("data_input_time", -1)
                           .skip(NEWS_COUNT_IN_PAGE * (page_Idx - 1))
                           .limit(NEWS_COUNT_IN_PAGE * page_Idx))
    }
    
    return send_data

def get_news_category_list(page_Idx: int, category_name: str):
    find_query = {
        "news_category" : NEWS_CATEGORY[category_name], 
        "cotent_summary" : {"$ne" : ""}, 
        "keyword" : {"$ne" : ""}, 
        "news_sentiment" : {"$ne" : ""}
    }
    send_data = {
        "news_list" : list(collection
                           .find(find_query)
                           .sort("data_input_time", -1)
                           .skip(NEWS_COUNT_IN_PAGE * (page_Idx - 1))
                           .limit(NEWS_COUNT_IN_PAGE * page_Idx))
    }
    
    return send_data

def get_keyword_count(date: str):
    date = date_parser(date)
    pipeline = [
        {
            "$match": {
                "news_year" : {"$regex" : date["year"]}, 
                "news_month" : {"$regex" : date["month"]}, 
                "news_day" :{"$regex" : date["day"]}, 
                "keyword" : {"$ne" : ""}, 
                "news_sentiment" : {"$ne" : ""}
            }    
        },
        {"$unwind" : "$keyword"},
        {"$group": {
            "_id": "$keyword",
            "count": {"$sum": 1}
        }},
        {"$sort" : {"count" : -1}},
        {"$limit" : 10}
    ]

    result = collection.aggregate(pipeline)
    
    keyword_count_list = []
    
    for keyword in result:
        
        # find_news_query = {
        #     "news_year" : {"$regex" : date["year"]}, 
        #     "news_month" : {"$regex" : date["month"]}, 
        #     "news_day" :{"$regex" : date["day"]}, 
        #     "keyword": {"$in": [keyword["_id"]]}
        # }
        
        # top_news_link = collection.find_one(find_news_query, sort=[("data_input_time", -1)])["news_link"]
        
        keyword_data = {
            "keyword" : keyword["_id"],
            "count" : keyword["count"],
            # "link" : top_news_link
        }
        
        keyword_count_list.append(keyword_data)
    
    send_data = {
        "keyword_count_list" : keyword_count_list
    }
    
    return send_data

def get_stock_sentiment_count(date :str, stock_code :str):
    date = date_parser(date)
    pipeline = [
        {
            "$match": {
                "news_year" : {"$regex" : date["year"]}, 
                "news_month" : {"$regex" : date["month"]}, 
                "news_day" :{"$regex" : date["day"]},
                "stock_company": STOCK_CODE_LIST[stock_code],
                "keyword" : {"$ne" : ""}, 
                "news_sentiment" : {"$ne" : ""}
            }
        },
        {"$group": {
            "_id": "$news_sentiment",
            "count": {"$sum": 1}
        }}
    ]

    result = collection.aggregate(pipeline)
    
    keyword_data = {}
    
    for keyword in result:
        keyword_data.update({keyword['_id'] : keyword["count"]})

    send_data = {
        "stock_sentiment_count" : keyword_data
    }
    
    return send_data

def get_stock_keyword_count(date :str, stock_name :str, keyword_limit :int):
    date = date_parser(date)
    pipeline = [
        {
            "$match": {
                "news_year" : {"$regex" : date["year"]}, 
                "news_month" : {"$regex" : date["month"]}, 
                "news_day" :{"$regex" : date["day"]}, 
                "stock_company" : stock_name,
                "keyword" : {"$ne" : ""}, 
                "news_sentiment" : {"$ne" : ""}
            }
        },
        {"$unwind" : "$keyword"},
        {"$group": {
            "_id": "$keyword",
            "count": {"$sum": 1}
        }},
        {"$sort" : {"count" : -1}},
        {"$limit" : keyword_limit}
    ]

    result = collection.aggregate(pipeline)
    
    keyword_data_list = []
    
    for keyword in  result:
        if keyword["_id"] == stock_name:
            continue
        keyword_data = {
            "name" : keyword["_id"],
            "value" : keyword["count"]
        }
        keyword_data_list.append(keyword_data)
    
    collection.find_one()
        
    keywords_data = {
        "name" : stock_name,
        "children" : keyword_data_list
    }
        
    return keywords_data

def get_stock_news_count(date :str, stock_limit :int):
    date = date_parser(date)
    pipeline = [
        {
            "$match": {
                "news_year" : {"$regex" : date["year"]}, 
                "news_month" : {"$regex" : date["month"]}, 
                "news_day" :{"$regex" : date["day"]}, 
                "keyword" : {"$ne" : ""}, 
                "news_sentiment" : {"$ne" : ""}
            }
        },
        {
            "$group" : {
                "_id" : "$stock_company",
                "count" : {"$sum" : 1}
            }
        },
        {"$sort" : {"count" : -1}},
        {"$limit" : stock_limit}
    ]
    
    result = collection.aggregate(pipeline)
    
    return result

def get_bubble_chart_data(date :str, stock_limit :int, keyword_limit :int):
    show_bubble_chart_list = list(get_stock_news_count(date, stock_limit))
    
    bubble_chart_data_list = []
    
    for stock_name in show_bubble_chart_list:
        bubble_chart_data_list.append(get_stock_keyword_count(date, stock_name["_id"], keyword_limit))
    
    send_data = {
        "bubble_chart_data_list" : bubble_chart_data_list
    }
    
    return send_data
    
@app.get(
    path="/news/whole", 
    name="전체 뉴스 기사 생성" ,
    description="전체 뉴스 기사 리스트를 날짜 순으로 생성하는 API",
    response_model = News_List
)
def news_whole_list(page_Idx :int):
    try:
        if page_Idx < 1:
            return get_news_whole_list(1)
        else :    
            return get_news_whole_list(page_Idx)
    except ValidationError as exc:
        print(repr(exc.errors()[0]['type']))
        return get_news_whole_list(1)
    
@app.get(
    path="/news/stock/{stock_code}",
    name="특정 종목 뉴스 기사 생성",
    description="특정 종목 뉴스 기사 리스트를 날짜 순으로 생성하는 API",
    response_model = News_List
)
def news_stock_list(page_Idx: int, stock_code: str = Path(title="종목코드")):
    
    if stock_code in STOCK_CODE_LIST:
        if page_Idx < 1:
            return get_news_stock_list(1, stock_code)
        elif page_Idx >= 1:    
            return get_news_stock_list(page_Idx, stock_code)
        else:
            raise HTTPException(status_code=404, detail="Page_Idx not found") 
    else:
        raise HTTPException(status_code=404, detail="stock_code not found")

@app.get(
    path="/news/category/{category_name}",
    name="특정 카테고리 뉴스 기사 생성",
    description="특정 카테고리 뉴스 기사 리스트를 날짜 순으로 생성하는 API",
    response_model = News_List
)
def news_category_list(page_Idx: int, category_name: Catagory_List):
    
    if category_name in NEWS_CATEGORY:
        if page_Idx < 1:
            return get_news_category_list(1, category_name)
        elif page_Idx >= 1:    
            return get_news_category_list(page_Idx, category_name)
        else:
            raise HTTPException(status_code=404, detail="Page_Idx not found") 
    else:
        raise HTTPException(status_code=404, detail="category_name not found")

@app.get(
    path="/keyword/count",
    name="특정 날짜의 키워드별 개수",
    description="특정 날짜에 수집된 전체 키워드별 개수를 생성하는 API",
    response_model=Keyword_Data
)
def keyword_count(date: str):
    date_length = len(date)
    if date_length in [4, 6, 8]:
        return get_keyword_count(date)
    else:
        raise HTTPException(status_code=404, detail="date type is wrong")
    
@app.get(
    path="/sentiment/count/{stock_code}",
    name="종목마다의 감성 분석 요소별 개수",
    description="특정 날짜에 수집된 해당 종목 기사의 감성 분석 요소별 개수",
    response_model=Sentiment_Data
)
def stock_sentiment_count(date: str, stock_code: str = Path(title="종목코드")):
    
    date_length = len(date)
    if stock_code in STOCK_CODE_LIST:
        if date_length in [4, 6, 8]:
            return get_stock_sentiment_count(date, stock_code)
        else:
            raise HTTPException(status_code=404, detail="date type is wrong")
    else: 
        raise HTTPException(status_code=404, detail="stock_code not found")

@app.get(
    path="/keyword/bubble",
    name="bubble chart 구현을 위한 데이터",
    description="특정 날짜의 bubble chart 구현을 위한 데이터",
    response_model=Bubble_Chart_Data
)
def bubble_chart_data(date: str, stock_limit :int, keyword_limit :int):
    date_length = len(date)
    if date_length in [4, 6, 8]:
        return get_bubble_chart_data(date, stock_limit, keyword_limit)
    else:
        raise HTTPException(status_code=404, detail="date type is wrong")    

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8092)
