# job-crawler
VietNam jobs data crawler using Scrapy

## Installation
Use the package manager [pip](https://pip.pypa.io/en/stable/) to install requirements.
```bash
pip install requirements.txt
```

## Crawl web data

### Crawl Linkedln VietNam jobs

In LinkedlnSpider:
- Change location in api-url to crawl your location
- Set first-job-on-page to 0

Crawl job and save data to json file by:
```bash
scrapy crawl linkedin_jobs -O linkedin_jobs.json
```
### Note: 
Crawl by Linkedln api just for 1000 jobs, for more data have to use proxy or ask for Linkedln permission.

## Crawl CareerBuilder jobs

In CareerBuilderSpider :
- set initPage to 1

Crawl job and save data to json file by:
```bash
scrapy crawl careerbuilder_jobs -O careerbuilder_jobs.json
```

## Future improvement
- Use items and pipelines for processing data
- Use scheduler
- Setting proxy