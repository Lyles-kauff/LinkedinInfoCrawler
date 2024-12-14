import json
import urllib.parse
from typing import Any, Iterable
import scrapy
from scrapy.http import Request, Response
import re

input_file = 'directorydata.json'
desired_company_names = [
"Company name","Valerie Redhorse","Dennis C. Healey EA","Morel Construction Co.","Utah Clean Energy","Holland & Hart","Green Lantern Solar","Trico Electric Coop","Meridian Clean Energy","Fredrikson & Byron","Hope Community Capital","dGEN Energy Partners","U.S. Department of Agriculture","Greenday Finance","San Diego Community Power","East Bay Community Energy","National Cooperative Bank","Duncan","Valley Electric Association","LIUNA","Avisen Legal","City of San José","Solariant Capital","Michigan Saves","Capital Power","clean-dev.com","SMUD","Barr Engineering Co.","US Solar","Legacy Bank and Trust","University of Colorado Boulder","Sunpin Solar","Carbonvert","Electric Power Engineers","Wilson Sonsini Goodrich & Rosati","NTUA","NextEra Energy","JEPIC-USA","Renewable America","Jupiter Power","Copia Power","bluejenergy.com","syso.com","UAMPS","cacommunitypower.org","Home Run Financing","Redwood Coast Energy Authority","Wisconsin","Empowered Energy","Norton Rose Fulbright","National Renewable Energy Laboratory","hardestycpa.com","Ackerman CPAs","keslou.com","Genz Associates","Elective","Grace Hebert Curtis Architects","Aircuity","Grove Climate Group","Energy Optimizers, USA","Iowa Lakes Electric","Traverse City Light & Power","IOWN Renewable Energy","Seattle.gov","Colorado Clean Energy Fund","EightTwenty","Powell County High School","kma-studio.co","DNV","Birchfield Penuel Architects","Jordin Marshall","Fortress Power","Hussung Mechanical Contractors","Cornerstone Engineering","Generation Solar","Islamic Society of Orange County","Clean Energy Group","Skyview Ventures","Solops Solar","Davis Hill Development","Clearway Energy","Strategic Energy Solutions","Bernhard","Pedal Steel Solar","HED","Turner Construction","Michigan State Police","Coalition for Green Capital","Silfab Solar","Los Angeles, California","Polk Group","Harding, Shymanski & Company, P.S.C.","TMP Architecture","Anglin Reichmann Armstrong","Veolia North America","CEP Renewables","Evans Engineering and Consulting","Day & Zimmermann","Energy Transfer","5 Architecture","Af-Architect.com","Pathward","Bristol Bay Native","United Mechanical","Trane Technologies","heyglide.com","Glide","Hoefer Welker","Altman + Barrett Architects","Novele","LUX Speed Capital","Bartlett Hartley & Mulkey","Delta G Consulting Engineers","Metropolitan Transit Authority of Harris County","SitelogIQ","Nevada Clean Energy Fund","Camber","genesysgeo.com","University Place, WA","Georgia Institute of Technology","Town of Needham","ADW Architects","Wagner Murray","Progressive AE","Studio S Architecture","Insight Architects","Ragona Architecture & Design","Jenkins•Peer Architects","Neighboring Concepts","BB+M Architecture","Little","Perspectus Architecture","The Lawrence Group","Neumann Monson Architects","WDD Architects","French Architects","WER Architects","TAGGART / Architects","SCM Architects","Plunkett Raysich Architects,","genesishightech.com","Business Coaching VAs","NYSUT","Kansas Department of Administration","Indianapolis Public Library","Polk County Health","Prairie Engineers"
]  # Please make sure to check the spellings of the names given
company_urls = []

def get_url_by_company_name():
    try:
        with open(input_file, 'r') as json_file:
            data = json.load(json_file)
            
            # Ensure JSON is a list of dictionaries
            if not isinstance(data, list) or not all(isinstance(d, dict) for d in data):
                raise ValueError("JSON must be a list of dictionaries")

            for name in desired_company_names:
                for company_data in data:
                    if name in company_data:
                        raw_url = str(company_data[name]).strip()
                        
                        # Encode special characters in the URL
                        encoded_url = urllib.parse.quote(raw_url, safe=":/?#[]@!$&'()*+,;=")
                        
                        # Validate URL structure
                        if not urllib.parse.urlparse(encoded_url).scheme:
                            print(f"Invalid URL skipped: {raw_url}")
                            continue
                        
                        company_urls.append(encoded_url)
            
            print("Company URLs:", set(company_urls))
    except FileNotFoundError:
        print(f"Error: JSON file '{input_file}' not found.")
    except Exception as e:
        print(f"An error occurred while reading JSON file: {str(e)}")


class CompanyProfileScraperSpider(scrapy.Spider):
    name = 'company_profile_scraper'
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        get_url_by_company_name()

        if not company_urls:
            print("No company URLs found. Exiting spider.")
            raise ValueError("No URLs to scrape.")

        self.company_pages = list(set(company_urls.copy()))

    def start_requests(self):
        company_index_tracker = 0

        first_url = self.company_pages[company_index_tracker]
        yield scrapy.Request(url=first_url, callback=self.parse_response,
                             meta={'company_index_tracker': company_index_tracker})

    def parse_response(self, response):
        company_index_tracker = response.meta['company_index_tracker']
        print('********')
        print(f'Scraping page: {str(company_index_tracker + 1)} of {str(len(self.company_pages))}')
        print('********')

        company_item = {}

        company_item['company_name'] = response.css('.top-card-layout__entity-info h1::text').get(
            default='not-found').strip()

        # Updated code to handle followers count as a float then convert to int
        followers_text = response.xpath(
            '//h3[contains(@class, "top-card-layout__first-subline")]/span/following-sibling::text()').get()
        if followers_text:
            followers_count = followers_text.split()[0].strip().replace(',', '')
            try:
                company_item['linkedin_followers_count'] = int(float(followers_count))  # Convert to float first then to int
            except ValueError:
                company_item['linkedin_followers_count'] = 0  # Default to 0 if conversion fails

        # Handle logo extraction
        company_item['company_logo_url'] = response.css(
            'div.top-card-layout__entity-image-container img::attr(data-delayed-url)').get('not-found')

        # Handle 'About Us' section
        company_item['about_us'] = response.css('.core-section-container__content p::text').get(
            default='not-found').strip()

        try:
            followers_num_match = re.findall(r'\d{1,3}(?:,\d{3})*',
                                             response.css('a.face-pile__cta::text').get(default='not-found').strip())
            if followers_num_match:
                company_item['num_of_employees'] = int(followers_num_match[0].replace(',', ''))
            else:
                company_item['num_of_employees'] = response.css('a.face-pile__cta::text').get(
                    default='not-found').strip()
        except Exception as e:
            print(f"Error occurred while getting number of employees: {e}")
            company_item['num_of_employees'] = 'not-found'  # Fallback value for errors

        try:
            company_details = response.css('.core-section-container__content .mb-2')

            company_item['website'] = company_details[0].css('a::text').get(default='not-found').strip()

            company_industry_line = company_details[1].css('.text-md::text').getall()
            company_item['industry'] = company_industry_line[1].strip()

            company_size_line = company_details[2].css('.text-md::text').getall()
            company_item['company_size_approx'] = company_size_line[1].strip().split()[0]

            company_headquarters = company_details[3].css('.text-md::text').getall()
            if company_headquarters[0].lower().strip() == 'headquarters':
                company_item['headquarters'] = company_headquarters[1].strip()
            else:
                company_item['headquarters'] = 'not-found'

            company_type = company_details[4].css('.text-md::text').getall()
            company_item['type'] = company_type[1].strip()

            # Specialties or founded handling
            unsure_parameter = company_details[5].css('.text-md::text').getall()
            unsure_parameter_key = unsure_parameter[0].lower().strip()
            company_item[unsure_parameter_key] = unsure_parameter[1].strip()

            if unsure_parameter_key == 'founded':
                company_specialties = company_details[6].css('.text-md::text').getall()
                if company_specialties[0].lower().strip() == 'specialties':
                    company_item['specialties'] = company_specialties[1].strip()
                else:
                    company_item['specialties'] = 'not-found'
            elif unsure_parameter_key != 'specialties' or unsure_parameter_key == 'founded':
                company_item['founded'] = 'not-found'
                company_item['specialties'] = 'not-found'

            # Funding parameters
            company_item['funding'] = response.css('p.text-display-lg::text').get(default='not-found').strip()

            # Safely handle funding_total_rounds field
            funding_rounds_text = response.xpath(
                '//section[contains(@class, "aside-section-container")]/div/a[contains(@class, "link-styled")]//span[contains(@class, "before:middot")]/text()').get('not-found').strip()
            if funding_rounds_text != 'not-found':
                try:
                    company_item['funding_total_rounds'] = int(funding_rounds_text.split()[0])
                except ValueError:
                    company_item['funding_total_rounds'] = 0  # Default to 0 if conversion fails
            else:
                company_item['funding_total_rounds'] = 0  # Default to 0 if not found

            company_item['funding_option'] = response.xpath(
                '//section[contains(@class, "aside-section-container")]/div//div[contains(@class, "my-2")]/a[contains(@class, "link-styled")]/text()').get(
                'not-found').strip()
            company_item['last_funding_round'] = response.xpath(
                '//section[contains(@class, "aside-section-container")]/div//div[contains(@class, "my-2")]/a[contains(@class, "link-styled")]//time[contains(@class, "before:middot")]/text()').get(
                'not-found').strip()

        except IndexError:
            print("Error: *****Skipped index, as some details are missing*********")

        yield company_item

        company_index_tracker += 1

        if company_index_tracker <= len(self.company_pages) - 1:
            next_url = self.company_pages[company_index_tracker]
            yield scrapy.Request(url=next_url, callback=self.parse_response,
                                 meta={'company_index_tracker': company_index_tracker})
