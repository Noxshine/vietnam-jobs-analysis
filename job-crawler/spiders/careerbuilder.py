import scrapy
import json
# from kafka import KafkaProducer


class CareerbuilderSpider(scrapy.Spider):
    name = "careerbuilder"
    api_url = 'https://careerbuilder.vn/viec-lam/tat-ca-viec-lam-trang-{}-vi.html'

    # Kafka
    # producer = KafkaProducer(
    #     bootstrap_servers=['localhost:9092'],
    #     value_serializer=lambda v: json.dumps(v).encode('utf-8')
    # )

    def start_requests(self):
        initPage = 1
        first_url = self.api_url.format(initPage)
        yield scrapy.Request(url=first_url, callback=self.parse_job, meta={'pageNum': initPage})

    def parse_job(self, response):

        jobs = response.css("div.job-item")

        for job in jobs:
            job_item = {}

            job_item['job_id'] = job.css('::attr(id)').get()
            job_item['job_title'] = job.css("h2 a::text").get(default='not-found').strip()
            # job_item['job_detail_url'] = job.css(".base-card__full-link::attr(href)").get(default='not-found').strip()
            job_item['job_listed'] = job.css('div.time time::text').get(default='not-found').strip()
            job_item['job_deadline'] = job.css('div.expire-date p::text').get(default='not-found').strip()
            job_item['salary'] = job.css('div.salary p::text').get(default='not-found').strip()
            job_item['company_name'] = job.css('a.company-name::text').get(default='not-found').strip()

            job_item['job_address'] = job.css('div.location li::text').get(default='not-found').strip()

            # joblink = job.css(".base-card__full-link::attr(href)").get(default='not-found').strip()
            joblink = job.css('div.title h2 a.job_link::attr(href)').get(default='not-found')
            if joblink != 'not-found':
                yield scrapy.Request(url=joblink, callback=self.parse_job_detail, meta={'jobitem': job_item})

        pageNum = response.meta['pageNum']
        if pageNum < 3:
            pageNum = pageNum + 1
            next_page = self.api_url.format(pageNum)
            if next_page:
                yield scrapy.Request(url=next_page, callback=self.parse_job, meta={'pageNum': pageNum})

    def parse_job_detail(self, response):
        jobitem = response.meta['jobitem']
        jobDetail = response.css("section.job-detail-content")

        jobDetail1 = jobDetail.css("div.bg-blue")

        jobitem['job_experience_required'] = jobDetail1.css('div.detail-box li:contains("Kinh nghiệm") p::text').get(
            default='not-found').strip()
        jobitem['job_experience_required'] = jobitem['job_experience_required'].replace('\r\n', '')

        jobitem['employment_type'] = jobDetail1.css('div.detail-box li:contains("Hình thức") p::text').get(
            default='not-found').strip()

        jobitem['job_function'] = jobDetail1.css('div.detail-box li:contains("Cấp bậc") p::text').get(
            default='not-found').strip()

        industriesText = jobDetail1.css('div.detail-box li:contains("Ngành nghề") p a::text').getall()
        jobitem['industries'] = ', '.join([industry.strip() for industry in industriesText])

        welfareText = jobDetail.css('ul.welfare-list li::text').getall()
        jobitem['welfare'] = ', '.join([welfare.strip() for welfare in welfareText])

        description_div = response.xpath('//div[@class="detail-row reset-bullet"]')
        job_description = description_div.xpath('.//p/text()').extract()
        jobitem['job_description'] = ', '.join([description.strip() for description in job_description])
        jobitem['job_description'] = jobitem['job_description'].replace('•', '')
        jobitem['job_description'] = jobitem['job_description'].replace('-', '')

        requirements_div = response.xpath('//div[@class="detail-row" and @reset-bullet=""]')
        job_requirements = requirements_div.xpath('.//p/text()').extract()
        jobitem['job_requirement'] = ', '.join([requirements.strip() for requirements in job_requirements])

        # kafka
        # self.producer.send('careerbuilder', value=jobitem)  # change to your topic

        yield jobitem

    # def closed(self, reason):
        # Close the Kafka producer when the spider is closed
        # self.producer.close()
