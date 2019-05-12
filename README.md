# lagou
import json
import math
import time
import tkinter
import random
import pymongo
import requests
import asyncio
import aiohttp
import multiprocessing
from pyppeteer import launch
from queue import Queue

START_URL = 'https://www.lagou.com/'
LOGIN_URL = 'https://www.lagou.com/frontLogin.do'
BASE_URL = 'https://www.lagou.com/jobs/positionAjax.json?needAddtionalResult=false'
WORK_INFO_URL = 'https://www.lagou.com/jobs/{}.html'
COMPANY_INFO_URL = 'https://www.lagou.com/gongsi/{}.html'

UA_LIST=[
    'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6',
    'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11',
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; Media Center PC 6.0; InfoPath.3; MS-RTC LM 8; Zune 4.7)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; Media Center PC 6.0; InfoPath.3; MS-RTC LM 8; Zune 4.7",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; Zune 4.0; InfoPath.3; MS-RTC LM 8; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 2.0.50727; SLCC2; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; Zune 4.0; Tablet PC 2.0; InfoPath.3; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0"
    ]

PROXY_URL = 'http://127.0.0.1:5555/random'

MAX_WORKER_NUM = multiprocessing.cpu_count()

def generate_mongodb():
    """生成一个MongoDB对象"""
    client = pymongo.MongoClient(host='localhost', port=27017)
    db = client.lagou
    return db

def get_random_proxy(proxy_url):
    try:
        response = requests.get(proxy_url)
        print('正在获取代理')
        if response.status_code == 200:
            proxy = response.text
            print('成功获取代理%s' % proxy)
            return proxy
    except Exception as e:
        print('获取代理失败', e.args)
        return '222.186.45.116:55756'

def get_screen_size():
    """
    使用tkinter获取屏幕大小
    :return:
    """
    tk = tkinter.Tk()
    width = tk.winfo_screenwidth()
    height = tk.winfo_screenheight()
    tk.quit()
    return width, height

async def login_lagou():
    """Pyppeteer登录拉勾"""
    browser = await launch({'args': ['--no-sandbox', '--disable-infobars']})
    # 开启选项卡
    page = await browser.newPage()
    width, height = get_screen_size()
    await page.setViewport(viewport={'width': width, 'height': height})
    await page.setJavaScriptEnabled(enabled=True)
    await page.setUserAgent(random.choice(UA_LIST))
    await page.goto('https://www.lagou.com/frontLogin.do')

    await page.type('input[type="text"]', 'phonenumber', {'delay': random.randint(100, 151) - 50})
    await page.type('input[type="password"]', 'password', {'delay': random.randint(100, 151) - 50})
    await asyncio.sleep(1)
    submit = await page.querySelector('input[type="submit"]')
    await submit.click()

    return browser, page

async def get_cookies(page):
    await page.goto(START_URL)
    cookies_list = await page.cookies()
    cookies = ''
    for cookie in cookies_list:
        cookies += '{}={}; '.format(cookie['name'], cookie['value'])
    await page.close()
    #cookies = {i['name']: i['value'] for i in cookies_list}
    return cookies

async def close_browser(browser):
    pages = await browser.pages()
    for page in pages:
        await page.close()
    await browser.close()

async def get_company_info(queue):

    browser, page = await login_lagou()
    db = generate_mongodb()

    while True:
        if queue.empty():
            break
        company_id = queue.get()
        try:
            width, height = get_screen_size()
            await page.setViewport(viewport={'width': width, 'height': height})
            await page.setJavaScriptEnabled(enabled=True)
            await page.setUserAgent(random.choice(UA_LIST))
            await page.goto(COMPANY_INFO_URL.format(company_id))
            #await page.waitForNavigation()
            company_info = {
                'label': await (await (await page.xpath('//div[@class="item_content"]/ul/li[1]/span'))[0].getProperty('textContent')).jsonValue(),
                'inancing': await (await (await page.xpath('//div[@class="item_content"]/ul/li[2]/span'))[0].getProperty('textContent')).jsonValue(),
                'scale': await (await (await page.xpath('//div[@class="item_content"]/ul/li[3]/span'))[0].getProperty('textContent')).jsonValue(),
                'address': await (await (await page.xpath('//div[@class="item_content"]/ul/li[4]/span'))[0].getProperty('textContent')).jsonValue(),
                'quote': await (await (await page.xpath('//div[@class="company_word"]'))[0].getProperty('textContent')).jsonValue(),
                'positionNum': await (await (await page.xpath('//div[@class="company_data"]/ul/li[1]/strong'))[0].getProperty('textContent')).jsonValue(),
                'resume_processing_time_rate': await (await (await page.xpath('//div[@class="company_data"]/ul/li[2]/strong'))[0].getProperty('textContent')).jsonValue(),
                'resume_processing_duration': await (await (await page.xpath('//div[@class="company_data"]/ul/li[3]/strong'))[0].getProperty('textContent')).jsonValue(),
                'interview_comment': await (await (await page.xpath('//div[@class="company_data"]/ul/li[4]/strong'))[0].getProperty('textContent')).jsonValue(),
                'last_login': await (await (await page.xpath('//div[@class="company_data"]/ul/li[5]/strong'))[0].getProperty('textContent')).jsonValue(),
                'introduction': await (await (await page.xpath('//span[@class="company_content"]'))[0].getProperty('textContent')).jsonValue(),
            }
            name = await (await (await page.xpath('//div[@class="company_main"]/h1/a'))[0].getProperty('textContent')).jsonValue()
            company_info['name'] = name
            db.company_info.insert(dict(company_info))
            print('%s公司信息已采集入库' % name)
            queue.task_done()
        except Exception as e:
            print('遇到异常', e.args) 
            print('切换窗口重新爬取')
            queue.put(company_id)
            await page.close()
            page = await browser.newPage()
            await asyncio.sleep(3)
        #await asyncio.sleep(3)
    pages = await browser.pages()
    for page in pages:
        await page.close()
    await browser.close()
    queue.join()

async def get_work_info(queue):
    browser, page = await login_lagou()
    db = generate_mongodb()

    while True:
        if queue.empty():
            break
        work_id = queue.get()
        try:
            width, height = get_screen_size()
            await page.setViewport(viewport={'width': width, 'height': height})
            await page.setJavaScriptEnabled(enabled=True)
            await page.setUserAgent(random.choice(UA_LIST))
            await page.goto(COMPANY_INFO_URL.format(work_id))
            await page.goto(WORK_INFO_URL.format(work_id))
            work_info = {
                'id': work_id,
                'content': await (await (await page.xpath('//dl[@id="job_detail"]'))[0].getProperty('textContent')).jsonValue()
            }
            name = await (await (await page.xpath('//span[@class="name"]'))[0].getProperty('textContent')).jsonValue()
            work_info['name'] = name
            db.work_info.insert(work_info)
            print('%s职位详细信息已入库' % name)
            queue.task_done()
        except Exception as e:
            print('遇到异常', e.args)
            print('切换窗口重新爬取')
            queue.put(work_id)
            await page.close()
            page = await browser.newPage()
            await asyncio.sleep(3)
        # await asyncio.sleep(3)
    pages = await browser.pages()
    for page in pages:
        await page.close()
    await browser.close()
    queue.join()

async def get_info(num):
    browser, page = await login_lagou()
    db = generate_mongodb()

    headers = {
        'Referer': 'https://www.lagou.com/jobs/list_Python?px=default&city=%E5%85%A8%E5%9B%BD',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    }

    data = {
        'first': 'false',
        'pn': num,
        'kd': 'python'
    }

    # 获取cookie身份
    cookies = await get_cookies(page)
    print('使用cooki: %s' % cookies)
    headers['Cookie'] = cookies

    # 模拟随机浏览器
    headers['User-Agent'] = random.choice(UA_LIST)

    # 使用代理
    #proxy = 'http://{}'.format(get_random_proxy(PROXY_URL))

    while True:
        try:
            async with aiohttp.ClientSession(headers=headers) as session:
                print('正在抓取第{}页'.format(num))
                response = await session.post(BASE_URL, data=data)
                result = json.loads(await response.text())
                await session.close()
                print(result)
                if result['success']:
                    collection = db.works
                    datas = result['content']['positionResult']['result']
                    # 拉勾网最多只显示前30页，本次搜索结果最大页数向上取整。
                    if math.ceil(result['content']['positionResult']['totalCount']/15) > 30:
                        MAX_PAGE_NUM = 30
                    else:
                        MAX_PAGE_NUM = math.ceil(result['content']['positionResult']['totalCount']/15)
                    collection.insert(datas)
                    #print('页面信息已入库')
                    for data in datas:
                        # 将发布该条职位信息的公司ID放入companies队列
                        company_id = data['companyId']
                        companies.put(company_id)
                        # 将职位ID放入works队列
                        work_id = data['positionId']
                        works.put(work_id)
                    sleep_time = random.randint(1, 3)
                    print('第{}页采集完毕，休眠{}秒后继续抓取下一页'.format(num, sleep_time))
                    await asyncio.sleep(sleep_time)
                    break
        except Exception as e:
            print('采集遇到异常: ', e.args)
            print('切换身份，休眠10秒重新采集')

            # 更新cookies身份
            cookies = await get_cookies(page)
            print('更新cookies为：{}'.format(cookies))
            headers['Cookie'] = cookies

            # 切换浏览器身份
            headers['User-Agent'] = random.choice(UA_LIST)

    await browser.close()

if __name__ == '__main__':
    # 存放公司ID的列表
    companies = Queue()
    # 存放岗位ID的列表
    works = Queue()

    # 最大页数
    MAX_PAGE_NUM = 30

    tasks = [asyncio.ensure_future(get_info(num)) for num in range(1, MAX_PAGE_NUM + 1)]
    loop = asyncio.get_event_loop()
    print('开始采集页面信息')
    loop.run_until_complete(asyncio.wait(tasks))

    # 启动一个进程池采集公司信息
    loop_company = asyncio.get_event_loop()
    print('开始采集公司信息')
    loop_company.run_until_complete(get_company_info(companies))

    # 启动一个进程池采集岗位信息
    loop_work = asyncio.get_event_loop()
    print('开始采集岗位信息')
    loop_work.run_until_complete(get_company_info(works))
