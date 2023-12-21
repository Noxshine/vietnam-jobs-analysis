import scrapy
import re
import json
from kafka import KafkaProducer


class CareerlinkSpider(scrapy.Spider):
    name = "careerlink"

    api_url = 'https://www.careerlink.vn/vieclam/list?page={}'

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],  # change to actual Kafka bootstrap servers and topic.
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    def start_requests(self):
        pages = []
        for i in range(1, 438):
            domain = self.api_url.format(i)
            yield scrapy.Request(url=domain, callback=self.parse_link)

    def parse_link(self, response):
        for i in range(1, 51):
            str = 'body > div.container.mt-3.mt-lg-4 > div > div.col-lg-9.px-0.px-lg-3 > ul > li:nth-child({}) > div ' \
                  '> div.media-body.overflow-hidden > a.job-link.clickable-outside::attr(href)'.format(i)
            updateDateStr = 'body > div.container.mt-3.mt-lg-4 > div > div.col-lg-9.px-0.px-lg-3 > ul > li:nth-child(' \
                            '{}) > div > div.media-body.overflow-hidden > div.d-lg-flex.justify-content-between > ' \
                            'div.align-items-center.justify-content-between > span > span::text'.format(i)
            deadlineDateStr = '#main > div.container.mt-24.bg-white.mb-40 > div.list-job > div.job-body.row > ' \
                              'div.col-md-8 > div.job-list-2 > div:nth-child({}) > div > div.body > div.box-info > ' \
                              'div.label-content > label.time > strong::text'.format(i)
            updateDate = response.css(updateDateStr).get(default='not-found').strip()
            deadlineDate = response.css(deadlineDateStr).get(default='not-found').strip()
            link = 'https://www.careerlink.vn' + response.css(str).get()
            yield scrapy.Request(url=link, callback=self.parse_job,
                                 meta={'link': link, 'update': updateDate, 'deadline': deadlineDate})

    def parse_job(self, response):
        job_item = {}
        # header-job-info > h1 > a
        # header-job-info > h1 > a
        url_parts = response.url.split('/')
        job_item['job_id'] = url_parts[-1].split('?')[0]
        job_item['job_title'] = response.css(
            '#jd-col > div > div.card.border-0.font-nunitosans.px-4 > div.job-detail-header.mt-3 > div.media.row.m-0 '
            '> div.media-body.job-title-and-org-name > div > h1::text').get(
            default='').strip()

        # jd-col > div > div.card.border-0.font-nunitosans.px-4 > div.job-detail-header.mt-3 > div.job-overview.mt-2 > div:nth-child(6) > div.date-from.d-flex.align-items-center > span
        # jd-col > div > div.card.border-0.font-nunitosans.px-4 > div.job-detail-header.mt-3 > div.job-overview.mt-2 > div:nth-child(4) > div.date-from.d-flex.align-items-center > span
        raw_listed = response.css(
            '#jd-col div.job-detail-header.mt-3 div.job-overview.mt-2 div.date-from.d-flex.align-items-center span').get()
        job_item['job_listed'] = re.search(r'\b(\d{1,2}-\d{1,2}-\d{4})\b', raw_listed).group(1)

        job_item['job_deadline'] = response.css(
            '#jd-col div.card.border-0.font-nunitosans.px-4 div.job-detail-header.mt-3 div.job-overview.mt-2 '
            'div.day-expired.d-flex.align-items-center span b::text').get(
            default='not-found').strip()
        job_item['job_deadline'] = job_item['job_deadline'].replace('\n', ' ')

        job_item['salary'] = response.css(
            '#jd-col div div.card.border-0.font-nunitosans.px-4 div.job-detail-header.mt-3 div.job-overview.mt-2 '
            'span.text-primary::text').get(
            default='not-found').strip()

        job_item['company_name'] = response.css(
            '#jd-col > div > div.card.border-0.font-nunitosans.px-4 > div.job-detail-header.mt-3 > div.media.row.m-0 '
            '> div.media-body.job-title-and-org-name > p > a > span::text').get(
            default='not-found').strip()

        # job_item['company_link'] = job.css('h4 a::attr(href)').get(default='not-found')
        locationText = response.css('#jd-col div.job-overview.mt-2 div.d-flex.align-items-start.mb-2 a::text').getall()
        job_item['job_address'] = ', '.join(
            [location.strip().replace('\n', ' ') for location in locationText if location.strip()])

        job_item['job_experience_required'] = response.css(
            '#jd-col > div > div.card.border-0.font-nunitosans.px-4 > div.job-detail-header.mt-3 > '
            'div.job-overview.mt-2 > div:nth-child(3) > span::text').get(
            default='not-found').strip()

        job_item['employment_type'] = response.css(
            '#section-job-summary > div.row.job-summary.d-flex > div.col-6.pr-1.pl-3.pr-md-2 > div > div > '
            'div:nth-child(1) > div > div.font-weight-bolder::text').get(
            default='not-found').strip()

        job_item['job_function'] = response.css(
            '#section-job-summary > div.row.job-summary.d-flex > div.col-6.pr-1.pl-3.pr-md-2 > div > div > '
            'div:nth-child(2) > div > div.font-weight-bolder::text').get(
            default='not-found').strip()

        job_item['education_level'] = response.css(
            '#section-job-summary > div.row.job-summary.d-flex > div.col-6.pr-1.pl-3.pr-md-2 > div > div > '
            'div:nth-child(3) > div > div.font-weight-bolder::text').get(
            default='not-found').strip()

        job_item['gender'] = response.css(
            '#section-job-summary > div.row.job-summary.d-flex > div.col-6.pl-1.pr-3.pl-md-2 > div > div > '
            'div.d-flex.align-items-baseline.label.mb-3 > div > div.font-weight-bolder::text').get(
            default='not-found').strip()

        industriesText = response.css(
            '#section-job-summary div.row.job-summary.d-flex div.col-6.pl-1.pr-3.pl-md-2 div a span::text').getall()
        job_item['industries'] = ', '.join([industry.strip() for industry in industriesText if industry.strip()])

        descriptionText = response.css('#section-job-description p::text').getall()
        job_item['job_description'] = ', '.join(
            [description.strip() for description in descriptionText if description.strip()])

        skillText = response.css('#section-job-skills div.raw-content.rich-text-content p::text').getall()
        job_item['skill'] = ', '.join([skill.strip() for skill in skillText if skill.strip()])

        self.producer.send('careerlink', value=job_item)  # change to your topic

        yield job_item

    def closed(self, reason):
        # Close the Kafka producer when the spider is closed
        self.producer.close()
