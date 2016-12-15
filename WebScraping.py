from lxml import html
import requests
import json
import datetime

def getMediaDate(url):

    page = requests.get(url)

    '''
    for line in page.iter_lines():
        if line:
            decoded_line = line.decode('utf-8')
            print(decoded_line)
    '''




    tree = html.fromstring(page.content)
    script = tree.xpath('/html/body/script[3]/text()')[0]

    #print(script)

    name, content = script.split("=", 1)

    json_object = json.loads(content[:-1])


    #print(json_object)
    #print(json_object['entry_data']['PostPage'][0]['media']['date'])
    timestamp = json_object['entry_data']['PostPage'][0]['media']['date']

    date = datetime.datetime.fromtimestamp(timestamp)

    #print(date)
    return date