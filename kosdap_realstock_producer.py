import os
import json
import requests
import time
#kafka producer 예제
from kafka import KafkaProducer

import tenacity

try:
    import websocket

except ImportError:
    print("websocket-client 설치중입니다.")
    os.system('python3 -m pip3 install websocket-client')
    
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
topic = "stock_api"
pr = MessageProducer(brokers, topic)

# 종목객체 담기
kosdaq_stock_list = []

# 웹소켓 접속키 발급
def get_approval(key, secret):
    # url = https://openapivts.koreainvestment.com:29443' # 모의투자계좌     
    url = 'https://openapi.koreainvestment.com:9443' # 실전투자계좌
    headers = {"content-type": "application/json"}
    body = {"grant_type": "client_credentials",
            "appkey": key,
            "secretkey": secret}
    PATH = "oauth2/Approval"
    URL = f"{url}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(body))
    approval_key = res.json()["approval_key"]
    return approval_key    

# 전역변수 선언

STOCK_CODE = {
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
    '141080': '레고켐바이오',    '365340': '성일하이텍',    '195940': 'HK이노엔'}

# kosdaq_stock = kosdaq_stock_number()
kosdaq_stock = ['247540', '086520', '091990', '022100', '066970', 
                '028300', '196170', '068760', '035900', '277810', 
                '403870', '058470', '263750', '214150', '293490', 
                '357780', '041510', '039030', '145020', '005290', 
                '095340', '112040', '240810', '036930', '253450', 
                '035760', '000250', '086900', '121600', '214370', 
                '067310', '393890', '025900', '034230', '237690', 
                '078600', '048410', '141080', '365340', '195940']

print(f"kosdaq_stock :\n {kosdaq_stock}")

# 장지웅
# i_appkey    = "PSVjw9Ad34r8xMvrhnloPqrAAPH1De9wYUBU"
# i_appsecret = "QaNwd1ktB94W3cvo5qQcpZaLyagCHi2YaxMLOlpfcwvQceLoBHGDKmdbDuuU8F2d8vxFW3H5L3RBNuWgy/rSJzNstmVy4ebqXjfuiV5selELD+JhICV4wHiZJHG8A03Gw85pCl4lWLcaTBmjnWsig1ilHpaQ4tDLWfnUog/enG/8Yq4Ccq8="

#최병훈
i_appkey = "PS3TNbWvHECUl7xmHAHjXQKRL2aeHhgW6RdW"
i_appsecret = "kde5wWjpIEgdm7G/c+Rrdd3n07LXATPWlopN3kbFHBWR9VpfJuBJK6nWjcWMGxadzmoz1H4bFAPq1zyFDwpkGQHPTXmeFZwTrRwGNm2GQHdsmkf5BLn61VxJmR9wU1/WznOrgwe21Vgwx5wSnz3v7buVDul0IZzGl7Il//hviDrW0QANwzE="
ACC_NO = "6901449801"
# websocket key

i_approval_key = get_approval(i_appkey, i_appsecret)

print("approval2_key [%s]" % (i_approval_key))

# 10개종목 돌리기
for i in range(0, 40):
    
    send_kosdaq = {
    "header": {"approval_key": i_approval_key, "custtype": "P", "tr_type": "1", "content-type": "utf-8"},
    "body": { "input": {"tr_id": "H0STCNT0",  # API명
                        "tr_key": kosdaq_stock[i]  # 종목번호
                       }
            }
    }

    kosdaq_stock_list.append(send_kosdaq)
    
# Pandas DataFrame 이용
def pdbind(result):
    tx = {
        'MKSC_SHRN_ISCD': result[0],
        'STCK_CNTG_HOUR': result[1],
        'STCK_PRPR': result[2],
        'PRDY_VRSS_SIGN': result[3],
        'PRDY_VRSS': result[4],
        'PRDY_CTRT': result[5],
        'WGHN_AVRG_STCK_PRC': result[6],
        'STCK_OPRC': result[7],
        'STCK_HGPR': result[8],
        'STCK_LWPR': result[9],
        'ASKP1': result[10],
        'BIDP1': result[11],
        'CNTG_VOL': result[12],
        'ACML_VOL': result[13],
        'ACML_TR_PBMN': result[14],
        'SELN_CNTG_CSNU': result[15],
        'SHNU_CNTG_CSNU': result[16],
        'NTBY_CNTG_CSNU': result[17],
        'CTTR': result[18],
        'SELN_CNTG_SMTN': result[19],
        'SHNU_CNTG_SMTN': result[20],
        'CCLD_DVSN': result[21],
        'SHNU_RATE': result[22],
        'PRDY_VOL_VRSS_ACML_VOL_RATE': result[23],
        'OPRC_HOUR': result[24],
        'OPRC_VRSS_PRPR_SIGN': result[25],
        'OPRC_VRSS_PRPR': result[26],
        'HGPR_HOUR': result[27],
        'HGPR_VRSS_PRPR_SIGN': result[28],
        'HGPR_VRSS_PRPR': result[29],
        'LWPR_HOUR': result[30],
        'LWPR_VRSS_PRPR_SIGN': result[31],
        'LWPR_VRSS_PRPR': result[32],
        'BSOP_DATE': result[33],
        'NEW_MKOP_CLS_CODE': result[34],
        'TRHT_YN': result[35],
        'ASKP_RSQN1': result[36],
        'BIDP_RSQN1': result[37],
        'TOTAL_ASKP_RSQN': result[38],
        'TOTAL_BIDP_RSQN': result[39],
        'VOL_TNRT': result[40],
        'PRDY_SMNS_HOUR_ACML_VOL': result[41],
        'PRDY_SMNS_HOUR_ACML_VOL_RATE': result[42],
        'HOUR_CLS_CODE': result[43],
        'MRKT_TRTM_CLS_CODE': result[44],
        'VI_STND_PRC': result[45],
    }
    
    return tx

parser_data_count = 0

def on_message(ws, data):
    if data[0] in ['0', '1']:  # 시세데이터가 아닌경우
        d1 = data.split("|")
        if len(d1) >= 4:
            isEncrypt = d1[0]
            tr_id = d1[1]
            tr_cnt = d1[2]
            recvData = d1[3]
            result = recvData.split("^")
# --------------------------------------------------- #메세지 보내는 구간 
            msg = pdbind(result)
            for key, value in STOCK_CODE.items():
                if key == msg["MKSC_SHRN_ISCD"]:
                    msg["COMPANY"] = value
            # print("msg :\n",msg)
            pr.send_message(msg, False)
#-----------------------------------------------------            
        else:
            print('Data Size Error=', len(d1))
    else:
        recv_dic = json.loads(data)
        tr_id = recv_dic['header']['tr_id']
        print("recv_dic\n", recv_dic)

        if tr_id == 'PINGPONG':
            send_ping = recv_dic
            ws.send(data, websocket.ABNF.OPCODE_PING)
            print("tr_id PINGPONG")
        else:  # parser data
            print('tr_id=', tr_id, '\nmsg=', data)
            global parser_data_count
            parser_data_count = parser_data_count + 1
        print("parser_data_count", parser_data_count)    
    
    # print("데이터 담음", result)
    # return result

def on_error(ws, error):
    print('error=', error)

def on_close(ws, status_code, close_msg):
    print('on_close close_status_code=', status_code, " close_msg=", close_msg)

def on_open(ws):
    # 10개종목 돌리기
    for companyCode in kosdaq_stock_list:
        ws.send(json.dumps(companyCode), websocket.ABNF.OPCODE_TEXT)  # 종목코드 3
        time.sleep(0.5)

@tenacity.retry( # 데코레이터 추가
wait=tenacity.wait_fixed(10), # wait 파라미터 추가
stop=tenacity.stop_after_attempt(5), # stop 파라미터 추가
)
def runWebsocket():
    ws = websocket.WebSocketApp("ws://ops.koreainvestment.com:21000",
                            on_open=on_open, on_message=on_message, on_error=on_error)
    ws.run_forever()
    raise ValueError("Errors make me stronger")
runWebsocket()