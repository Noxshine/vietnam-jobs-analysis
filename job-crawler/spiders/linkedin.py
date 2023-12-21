import scrapy
import requests
from bs4 import BeautifulSoup

location = [
    "location=Vietnam&geoId=104195383",

    "location=Hanoi%2C%20Hanoi%2C%20Vietnam&geoId=105790653",
    "location=Hanoi%20Capital%20Region&geoId=90010186",

    "location=Ho%20Chi%20Minh%20City%2C%20Ho%20Chi%20Minh%20City%2C%20Vietnam&geoId=102267004",
    "location=Biên%2BHòa%2C%2BDong%2BNai%2C%2BVietnam&geoId=103877324",

    "location=Đà%20Nang%2C%20Da%20Nang%20City%2C%20Vietnam&geoId=105668258",

    "location=Haiphong%2C%20Hai%20Phong%20City%2C%20Vietnam&geoId=102884955"
]


class LinkedJobsSpider(scrapy.Spider):
    name = "linkedin"
    api_url = 'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/' \
              'search?location=Haiphong%2C%20Hai%20Phong%20City%2C%20Vietnam&geoId=102884955' \
              '&trk=public_jobs_jobs-search-bar_search-submit&start='

    # start_urls = [self.api_url.format(loc) for loc in location]
    # start_urls = [api_url.format(location[0])]

    def start_requests(self):
        first_job_on_page = 0
        first_url = self.api_url + str(first_job_on_page)

        # headers = { 'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)
        # Chrome/79.0.3945.130 Safari/537.36' }

        yield scrapy.Request(url=first_url, callback=self.parse_job, meta={'first_job_on_page': first_job_on_page})

    def parse_job(self, response):
        first_job_on_page = response.meta['first_job_on_page']

        job_item = {}
        jobs = response.css("li")

        num_jobs_returned = len(jobs)
        print("******* Num Jobs Returned *******")
        print(num_jobs_returned)
        print('*****')

        for job in jobs:

            job_item['job_title'] = job.css("h3::text").get(default='not-found').strip()
            # job_item['job_detail_url'] = job.css(".base-card__full-link::attr(href)").get(default='not-found').strip()
            job_item['job_listed'] = job.css('time::text').get(default='not-found').strip()

            job_item['company_name'] = job.css('h4 a::text').get(default='not-found').strip()
            # job_item['company_link'] = job.css('h4 a::attr(href)').get(default='not-found')
            job_item['company_location'] = job.css('.job-search-card__location::text').get(default='not-found').strip()

            dataid = job.css('div.base-card::attr(data-entity-urn)').get()
            if dataid:
                jobid = dataid.split(":")[3]

                print("------------------------------")
                print(jobid)
                print("------------------------------")

                target_url = 'https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{}'
                item_url = target_url.format(jobid)

                resp = requests.get(item_url)
                soup = BeautifulSoup(resp.text, 'html.parser')

                items = soup.find("ul", {"class": "description__job-criteria-list"})

                if items:
                    items = items.find_all("li")
                    for item in items:
                        header = item.find('h3', {"class": "description__job-criteria-subheader"}).text.strip()
                        value = item.find('span', {"class": "description__job-criteria-text--criteria"}).text.strip()

                        job_item[header] = value

                item1 = soup.find("div", {"class": "show-more-less-html__markup"})
                if item1:
                    job_item['job_decription'] = item1.text.strip()

                # yield scrapy.Request(url=item_url, callback=self.parse_description_job, meta={'jobitem': job_item})

            yield job_item

        if num_jobs_returned > 0:
            first_job_on_page = int(first_job_on_page) + 25
            next_url = self.api_url + str(first_job_on_page)
            yield scrapy.Request(url=next_url, callback=self.parse_job, meta={'first_job_on_page': first_job_on_page})
    #
    # def parse_description_job(self, response):
    #     jobitem = response.meta['jobitem']
    #
    #     job_criteria = response.css("ul.description__job-criteria-list ")
    #     job_criterias = job_criteria.css('li.description__job-criteria-item')
    #     for criterias in job_criterias:
    #         header = criterias.css('h3.description__job-criteria-subheader::text').get()
    #         value = criterias.css('span.description__job-criteria-text::text').get()
    #         # header = criterias.find('h3', {"class": "description__job-criteria-subheader"}).text.strip()
    #         # value = criterias.find('span', {"class": "description__job-criteria-text--criteria"}).text.strip
    #
    #         jobitem[header] = value
    #
    #     job_description = response.css("div.show-more-less-html__markup.show-more-less-html__markup--clamp-after-5")
    #     jobitem['job_decription']= job_description.css('ul:nth-child(1) li::text').getall(default='not-found')
    #
    #     # Extract YÊU CẦU
    #     jobitem['requirements'] = job_description.css('ul:nth-child(2) li::text').getall(default='not-found')
    #
    #     # Extract QUYỀN LỢI ĐƯỢC HƯỞNG
    #     jobitem['benefits'] = job_description.css('ul:nth-child(3) li::text').getall(default='not-found')
