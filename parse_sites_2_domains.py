import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import fake_useragent
import certifi
from tqdm import tqdm
import concurrent.futures
import time

CONNECTIONS = 100
TIMEOUT = 1

urls_description = list()
urls = pd.read_csv('urls_2_domains.csv')
urls = list(urls['url'])
print('Размер списка urls', len(urls))
ua = fake_useragent.UserAgent()


def parse_url(url, timeout):

    URL_TEMPLATE = f"http://{url}"
    try:
        r = requests.get(URL_TEMPLATE, timeout=timeout)
        request_status =r.status_code
        #r.encoding = r.apparent_encoding
        #r.encoding = 'ISO-8859-1'
        r.encoding = 'UTF-8'

        if request_status == 200:
            soup = bs(r.text, 'lxml')

            title = soup.find('title')
            if title != None:
                title = title.get_text(strip=True, separator=" ")
            else:
                title = ""
            #print('title: ',title)

            description = soup.find('meta', attrs ={'name':['Description','description']})
            if description != None:
                description = description.attrs['content']
            else:
                description = ""
            #print('description: ',description)
            result_description = str(title)+" "+str(description)
        else:
            result_description = "Recuest_status"+" "+str(request_status)
    except Exception as exc:
        result_description = "Except"+" "+str(type(exc))

    return url, result_description


with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
    future_to_url = (executor.submit(parse_url, url, TIMEOUT) for url in urls)
    time1 = time.time()
    for future in tqdm(concurrent.futures.as_completed(future_to_url)):

        try:
            data = future.result()
        except Exception as exc:
            data = str(type(exc))
        finally:

            urls_description.append(data)

    time2 = time.time()

print(f'Took {time2-time1:.2f} s')
pd.DataFrame(data=urls_description, columns=('url','description')).to_csv("urls_2_domains_description.csv")