import scrapy
import re
import random
from datetime import datetime, timedelta

def convert_job_listed_to_date(job_listed):
    current_date = datetime.now()

    if 'phút' in job_listed:
        minutes_ago = int(job_listed.split()[2])
        converted_date = current_date - timedelta(minutes=minutes_ago)
    elif 'giờ' in job_listed:
        hours_ago = int(job_listed.split()[2])
        converted_date = current_date - timedelta(hours=hours_ago)
    elif 'ngày' in job_listed:
        days_ago = int(job_listed.split()[2])
        converted_date = current_date - timedelta(days=days_ago)
    elif 'tuần' in job_listed:
        weeks_ago = int(job_listed.split()[2])
        # Assuming a month is approximately 30 days for simplicity
        converted_date = current_date - timedelta(days=weeks_ago * 7 + random.randint(-2, 2))    
    elif 'tháng' in job_listed:
        months_ago = int(job_listed.split()[2])
        # Assuming a month is approximately 30 days for simplicity
        converted_date = current_date - timedelta(days=months_ago * 30 + random.randint(-10, 10))
    else:
        # Handle other cases or throw an error if needed
        return None
    return converted_date.strftime('%d/%m/%Y')

class TopcvSpider(scrapy.Spider):
    name = "topcv"
    api_url='https://www.topcv.vn/viec-lam-it?sort=high_salary&skill_id=&skill_id_other=&keyword=&company_field=&position=&salary=&page={}'
    def start_requests(self):
        pages = []
        for i in range(1,5):
            domain = self.api_url.format(i)
            yield scrapy.Request(url=domain, callback=self.parse_link) 

    def parse_link(self, response):   
        for i in range(1,51):
            str ='#main > div.container.mt-24.bg-white.mb-40 > div.list-job > div.job-body.row > div.col-md-8 > div.job-list-2 > div:nth-child({}) > div > div.body > div.title-block > h3 > a::attr(href)'.format(i)
            updateDateStr='#main > div.container.mt-24.bg-white.mb-40 > div.list-job > div.job-body.row > div.col-md-8 > div.job-list-2 > div:nth-child({}) > div > div.body > label::text'.format(i)
            deadlineDateStr='#main > div.container.mt-24.bg-white.mb-40 > div.list-job > div.job-body.row > div.col-md-8 > div.job-list-2 > div:nth-child({}) > div > div.body > div.box-info > div.label-content > label.time > strong::text'.format(i)
            updateDate= response.css(updateDateStr).get(default='not-found').strip()
            deadlineDate = response.css(deadlineDateStr).get(default='not-found').strip()
            link = response.css(str).get()
            yield scrapy.Request(url=link, callback=self.parse_job,meta={'update':updateDate})

    
        
    def parse_job(self, response):
        job_item = {}
        
        url_parts = response.url.split('/')
        job_item['post_id']= url_parts[-1].split('.')[0]

        job_item['job_title'] = response.css('h1.job-detail__info--title::text').get(default='').strip()
        job_item['job_title'] = job_item['job_title'] + ' ' + response.css("#header-job-info h1 a::text").get(default='').strip()

        job_item['job_listed'] = response.meta.get('update','')
        job_item['job_listed'] = convert_job_listed_to_date(job_item['job_listed'])

        job_item['salary'] = response.css('#header-job-info > div.job-detail__info--sections > div:nth-child(1) > div.job-detail__info--section-content > div.job-detail__info--section-content-value::text').get(default='not-found').strip()
        
        deadlineText = response.css('#header-job-info > div.job-detail__info--deadline').get()
        job_item['job_deadline'] = re.search(r'\b(\d{1,2}/\d{1,2}/\d{4})\b', deadlineText).group(1)

        job_item['company_name'] = response.css('#job-detail > div.job-detail__wrapper > div > div.job-detail__body-right > div.job-detail__box--right.job-detail__company > div.job-detail__company--information > div.job-detail__company--information-item.company-name > h2 > a::text').get(default='not-found').strip()
    
        job_item['job_address'] = response.css('#header-job-info > div.job-detail__info--sections > div:nth-child(2) > div.job-detail__info--section-content > div.job-detail__info--section-content-value::text').get(default='not-found').strip()

        job_item['job_experience_requied'] = response.css('#job-detail-info-experience > div.job-detail__info--section-content > div.job-detail__info--section-content-value::text').get(default = 'not-found').strip()

        job_item['employment_type'] = response.css('#job-detail > div.job-detail__wrapper > div > div.job-detail__body-right > div.job-detail__box--right.job-detail__body-right--item.job-detail__body-right--box-general > div > div:nth-child(4) > div.box-general-group-info > div.box-general-group-info-value::text').get(default = 'not-found').strip()

        job_item['job_function'] = response.css('#job-detail > div.job-detail__wrapper > div > div.job-detail__body-right > div.job-detail__box--right.job-detail__body-right--item.job-detail__body-right--box-general > div > div:nth-child(1) > div.box-general-group-info > div.box-general-group-info-value::text').get(default = 'other').strip()

        job_item['amount'] = response.css('#job-detail > div.job-detail__wrapper > div > div.job-detail__body-right > div.job-detail__box--right.job-detail__body-right--item.job-detail__body-right--box-general > div > div:nth-child(3) > div.box-general-group-info > div.box-general-group-info-value::text').get(default = 'other').strip()

        job_item['gender'] = response.css('#job-detail > div.job-detail__wrapper > div > div.job-detail__body-right > div.job-detail__box--right.job-detail__body-right--item.job-detail__body-right--box-general > div > div:nth-child(5) > div.box-general-group-info > div.box-general-group-info-value::text').get(default = 'other').strip()


        industriesText = response.css('#job-detail > div.job-detail__wrapper > div > div.job-detail__body-right > div.job-detail__box--right.job-detail__body-right--item.job-detail__body-right--box-category > div:nth-child(1) > div.box-category-tags > a::text').getall()
        job_item['Industries'] = ', '.join([industry.strip() for industry in industriesText])

        
        descriptionText = response.css('#box-job-information-detail > div.job-detail__information-detail--content > div > div:nth-child(1) > div > p::text').getall() + response.css('#box-job-information-detail > div.job-detail__information-detail--content > div > div:nth-child(1) > div > ul > li::text').getall()
        job_item['job_description']=', '.join([description.strip().replace('\xa0', ' ') for description in descriptionText if description.strip()])

        skillText = response.css('#box-job-information-detail > div.job-detail__information-detail--content > div > div:nth-child(2) > div > p::text').getall()+ response.css('#box-job-information-detail > div.job-detail__information-detail--content > div > div:nth-child(2) > div > ul > li::text').getall()
        job_item['skill']=', '.join([skill.strip().replace('\xa0', ' ')  for skill in skillText if skill.strip()])

        benefitText = response.css('#box-job-information-detail > div.job-detail__information-detail--content > div > div:nth-child(3) > div > p::text').getall()+response.css('#box-job-information-detail > div.job-detail__information-detail--content > div > div:nth-child(3) > div > ul > li::text').getall()
        job_item['benefit']=', '.join([benefit.strip().replace('\xa0', ' ') for benefit in benefitText if benefit.strip()])

        yield job_item
    
    
    

    