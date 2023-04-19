import requests

if __name__ == '__main__':
    url = "https://httpbin.org/ip"

    # proxy_addr = "140.246.80.189:50165"
    # authKey = "8F39DFEB"
    # password = "2D493ACF5DE2"

    # proxy_url = "http://%(user)s:%(password)s@%(server)s" % {
    #     "user": authKey,
    #     "password": password,
    #     "server": proxy_addr,
    # }

    # proxies = {
    #     "http": proxy_url,
    #     "https": proxy_url,
    # }

    resp = requests.get(url, proxies=None)
    print(resp.text)
