import requests
from redis import StrictRedis
import time


def get_proxy_ips():
    url = "https://proxyapi.horocn.com/api/v2/proxies?order_id=WYDT1682571017909269&num=10&format=text" \
          "&line_separator=unix&can_repeat=yes&user_token=098867c35aa52480685fa493268dbd06"
    return requests.get(url).text.split('\n')


def run():
    """每隔20秒更新一次代理池"""
    rconn = StrictRedis(host='localhost', port=6379, db=1, decode_responses=True)
    while True:
        print("Available IP Numbers: ", rconn.scard('proxy_pool'))
        if rconn.scard('proxy_pool') >= 100:
            time.sleep(60)
        else:
            for ip in get_proxy_ips():
                rconn.sadd('proxy_pool', ip)
            time.sleep(20)


if __name__ == '__main__':
    run()
