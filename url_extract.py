import csv
import requests
import time
import string
import datetime

from bs4 import BeautifulSoup

start_time = time.time()

def read_csv(path = './input_file/all_urls.csv'):
    with open(path, encoding='utf-8') as f:
        fobj = csv.DictReader(f)

        for row in fobj:
            yield row

def run(idx, base_url):
    html_page = requests.get(base_url)
    soup = BeautifulSoup(html_page.text, 'html.parser')
    links = []

    for link in soup.findAll('a', href=True):
        # skip useless links
        if link['href'] == '' or link['href'].startswith('#'):
            continue
        # initialize the link
        thisLink = {
            'url': link['href'],
            'title': link.string,
        }
        
        if thisLink['title'] is None:
            # check for text inside the link
            if len(link.contents):
                thisLink['title'] = ' '.join(link.stripped_strings)

        if thisLink['title'] is None:
            # if there's *still* no title (empty tag), skip it
            continue
        # convert to something immutable for storage
        hashable_link = (thisLink['url'].strip(),
                         thisLink['title'].strip())
        # store the result
        if hashable_link not in links:
            links.append(hashable_link)
    id = 1
    with open('./output_file/scraped_pr_links.csv','a' if idx else 'w', newline = '', encoding = 'utf-8') as fobj:
        column_names = ['id', 'url', 'title', 'source', 'time']
        fd = csv.DictWriter(fobj, fieldnames=column_names)

        if not idx:
            fd.writeheader()

        for link in links:
            a, b = link
            if a and b:
                try:
                    fd.writerow(dict(zip(column_names, [id, a, b, base_url, "%s seconds" % (time.time() - start_time)])))
                except UnicodeEncodeError:
                    print('Error with row, not saving row to csv - ', a, " ", b, " ", base_url)
                    continue


if __name__ == '__main__':
    links = read_csv()
    time_i = []

    for idx, alink in enumerate(links):

        try:
            n = "%s" % round((time.time() - start_time), 2)
            time_i.append(n)

            if idx == 0:
                print(idx, alink["\ufeffurls"], "--- %s seconds ---" % round(float(time_i[idx]),2))
                run(idx, alink["\ufeffurls"])

            else:
                print(idx, alink["\ufeffurls"], "--- %s seconds ---" % round((float(time_i[idx]) - float(time_i[idx - 1])),2))
                run(idx, alink["\ufeffurls"])

        # Skip all errors
        except:
            print("Error with link number - ", idx, " ", alink["\ufeffurls"])
            continue

    id = []
    url = []
    title = []
    source = []
    time = []
    ext_date = []

    with open('./output_file/scraped_pr_links.csv', newline = '', encoding = 'utf-8') as f:
        column_names = ['id', 'url', 'title', 'source', 'time']
        frdr = csv.DictReader(f, fieldnames=column_names)

        for row in frdr:

            url.append(row['url'])
            title.append(row['title'])
            source.append(row['source'])
            time.append(row['time'])

        last_row = len(url)

    for l in range(last_row):
        if l == 0:
            id.append('id')
            ext_date.append('extract date')
        else:
            id.append(l)
            ext_date.append(datetime.date.today())

    with open('./output_file/scraped_pr_links.csv', 'w', newline='', encoding='utf-8') as f:
        column_names = ['id', 'url', 'title', 'source', 'time', 'extract date']
        frtr = csv.DictWriter(f, fieldnames=column_names)

        for row in range(last_row):
            frtr.writerow({'id': id[row], 'url': url[row], 'title': title[row], 'source': source[row], 'time': time[row], 'extract date': ext_date[row]})