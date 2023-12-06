import pymongo
import json
import openai
from datetime import datetime
from kafka import KafkaProducer

client = pymongo.MongoClient('mongodb://sam9:sam9mo!!@59.3.28.12:27017/')
db = client["sam9mo"]
collection = db["news"]

openai.api_key = "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"

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

brokers = ['59.3.28.12:9092', '59.3.28.12:9093', '59.3.28.12:9094']

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

# chat gpt-3.5를 이용한 기사 내용 키워드 및 감성 분석
def gpt_process_keyword(content_summary):
    print("키워드 요약중.....")
    print("content_summary\n", content_summary)
    response = openai.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role" : "user", "content" :  f'({content_summary}) Respond with subject, object, and complement related to stock items from the sentence inside the following parentheses, prioritizing them by importance and limiting the response to an Array with a maximum length of 3. Additionally, provide sentiment analysis of the article summary'}],
        functions=[{
            "name" : "extract_keywords_news_sentiment",
            "description" : "Respond with subject, object, and complement related to stock items from the sentence inside the following parentheses, , provide sentiment analysis of the article summary.",
            "parameters" : {
                "type" : "object",
                "properties" : {
                    "keyword" : {
                        "type" : "array",
                        "description" : "Respond with subject, object, and complement related to stock items from the sentence inside the following parentheses, prioritizing them by importance and limiting the response to an Array with a maximum length of 3",
                        "items" : {
                            "type" : "string"
                        },
                        "maxItems" : 3
                    },
                    "news_sentiment" : {
                        "type" : "string",
                        "enum" : ["posivive", "negative", "neutral"],
                        "description" : "provide sentiment analysis of the article summary."
                    }
                },
                "required" : ["keyword", "news_sentiment"],
            }
        }],
    )

    result = json.loads(response.choices[0].message.function_call.arguments)
    print("result\n", result)
    return result


def gpt_process_index(date, count):

    find_keyword_sentiment_query = {
        "$or" : [
            {"keyword" : ""},
            {"keyword" : []},
            {"news_sentiment" : ""}
        ],
        "content_summary" : {"$ne" : ""}, 
        "data_input_time" : {"$regex" : f'{date}'}
    }
    try:
        doc_list = list(collection.find(find_keyword_sentiment_query).sort("data_input_time", -1))
    except Exception as ex:
        send_function_status(
            "news", 
            "extract_keyword&sentiment", 
            "Fail", 
            f'but process continue.\n {ex}'
        )
        print(ex)

    for i in range(count):
        input_collection = doc_list[i]
        content_summary = input_collection['content_summary']
        try:
            update_data = gpt_process_keyword(content_summary)
        except Exception as ex:
            send_function_status(
                "news", 
                "extract_keyword&sentiment", 
                "Fail", f'but process continue.\n {ex}',
                input_collection["_id"]
            )
            print(ex)

        find_id_query = {
            "_id" : input_collection['_id']
        }
        update_keyword_sentiment_query = {
            "$set" : {
                "keyword" : update_data['keyword'], 
                "news_sentiment" : update_data['news_sentiment']
            }
        }
        
        print(input_collection['_id'], update_data['keyword'], update_data['news_sentiment'])
        
        try:
            collection.update_one(find_id_query, update_keyword_sentiment_query, upsert=False)
        except Exception as ex:
            send_function_status(
                "news", 
                "extract_keyword&sentiment", 
                "Fail", 
                f'but process continue.\n {ex}', 
                input_collection["_id"]
            )
            print(ex)
            
def gpt_process_list(date):
    # find_keyword_sentiment_query = {"content_summary" : {"$ne" : ""}, "keyword" : "", "news_sentiment" : "", "data_input_time" : {"$regex" : f'{date}'}}
    find_keyword_sentiment_query = {
        "$or" : [
            {"keyword" : ""},
            {"keyword" : []}
        ],
        "content_summary" : {"$ne" : ""}, 
        "news_sentiment" : "", 
        "data_input_time" : {"$regex" : f'{date}'}
    }
    
    try:
        doc_list = list(collection.find(find_keyword_sentiment_query).sort("data_input_time", -1))
    except Exception as ex:
        send_function_status(
            "news", 
            "extract_keyword&sentiment", 
            "Fail", 
            f'but process continue.\n {ex}'
        )
        print(ex)

    for input_collection in doc_list:
        content_summary = input_collection['content_summary']

        try:
            update_data = gpt_process_keyword(content_summary)
        except Exception as ex:
            send_function_status(
                "news", 
                "extract_keyword&sentiment", 
                "Fail", f'but process continue.\n {ex}',
                input_collection["_id"]
            )
            print(ex)

        find_id_query = {"_id" : input_collection['_id']}
        update_keyword_sentiment_query = {
            "$set" : {
                "keyword" : update_data['keyword'], 
                "news_sentiment" : update_data['news_sentiment']
            }
        }
        print(input_collection['_id'], update_data['keyword'], update_data['news_sentiment'])
        try:
            collection.update_one(find_id_query, update_keyword_sentiment_query, upsert=False)
        except Exception as ex:
            send_function_status(
                "news", 
                "extract_keyword&sentiment", 
                "Fail", 
                f'but process continue.\n {ex}', 
                input_collection["_id"]
            )
            print(ex)

# gpt_process_index()
while True:
    current_time = datetime.now()
    current_year = current_time.year
    current_month = current_time.month
    current_day = current_time.day
    
    if len(str(current_month)) == 1:
        current_month = "0" + str(current_month)
    if len(str(current_day)) == 1:
        current_day = "0" + str(current_day)


    date = str(current_year) + str(current_month) + str(current_day)
    send_function_status(
        "news", 
        "extract_keyword&sentiment", 
        "Success"
    )
    gpt_process_list(date)