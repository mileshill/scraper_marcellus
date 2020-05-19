import os
import pymongo
import re
import scrapy
import scrapy_splash
from itertools import groupby
from scrapy import FormRequest


class ProductionReport(scrapy.Item):
    # Geographic
    county = scrapy.Field()
    township = scrapy.Field()

    # Table data about given well or pad
    well_name = scrapy.Field()
    estimated_royalties = scrapy.Field()
    wellhead_market_value = scrapy.Field()
    mcf = scrapy.Field()
    well_report_link = scrapy.Field()
    date_start = scrapy.Field()
    permit_number = scrapy.Field()
    production_report = scrapy.Field()


class MarcellusSpider(scrapy.Spider):
    name = "marcellus"
    BASE_URL = "http://www.marcellusgas.org"
    start_urls = ["http://www.marcellusgas.org/login.php"]
    collection = pymongo.MongoClient(host="localhost", port=27017)["marcellus"]["report.production"]

    def parse(self, response):
        # If you need a CSRF token, do it first
        return FormRequest.from_response(
            response,
            callback=self.start_scraping,
            formdata={
                "action": "login",
                "redirect": "pro_update.php",
                "EMAIL": os.getenv("EMAIL"),
                "PASSWORD": os.getenv("PASSWORD"),
            },
        )

    def start_scraping(self, response):
        yield scrapy_splash.SplashRequest(url=response.url, callback=self.parse_production_report, args={"wait": 2})

    def parse_production_report(self, response):
        counties = self.get_county_names(response)
        county_ids = self.get_county_ids(response)

        county_dict = dict()
        data = list()

        # Scrape the production report page to gather all the table data; county, township, well_name, link_to_report
        for county_idx, county in enumerate(county_ids):
            townships = self.get_townships_by_county_id(response, county)
            county_dict[county] = {"townships": townships}
            county_dict[county]["links"] = self.get_townships_link_by_county_id(response, county)
            for idx, link in enumerate(county_dict[county]["links"]):
                table_rows = self.get_table_rows(response, link)
                county_dict[county][townships[idx]] = table_rows
                # Get all the wells in the county/township
                for row in table_rows:
                    row["county"] = counties[county_idx]
                    row["township"] = townships[idx]
                # Update `data` with the wells from county/township
                data += table_rows

        # This is where the magic of all the data comes from!
        # Download each of the links to all the well reports and download the reports for those wells.
        for row in data:
            link = row["link"]
            yield scrapy.Request(url=f"{self.BASE_URL}{link}", callback=self.parse_well_report, meta={"row": row})

    def check_for_persisted(self, row):
        parsed_name = row["well_name"].replace(" ", "_").lower()
        record = self.collection.find_one({"well_name": parsed_name})
        return True if record is not None else False

    def parse_well_report(self, response):
        """
        Parse the production well report based on a given well_id.
        This is the callback function that execute for each Production Report - Well Report is followed
        :param response:  Result response object from the well_id-well production report
        :return:
        """
        # Split the URL where the parameters being.
        page = response.url.split(".php?")[-1]

        # Get the well_id from the remaining part of the URL
        well_id = re.match(r".*well_id=([0-9]+)", page).group(1)

        # Follow the link
        return self.parse_by_well_id(response, well_id, row=response.meta["row"])

    def parse_by_well_id(self, response, well_id, row):
        # Get the desired DOM element
        dom = response.xpath(f"//div[@id='pro_{well_id}']").extract()
        if len(dom) == 0:
            return

        # Sometimes errors occur
        try:
            # Extract the first (and only) element
            dom = dom[0]

            # Replace all the whitespace characters with null strings
            no_ws = re.sub(r"\s", "", dom)

            # Remove all the HTMl tags. Replace those tags with newline characters
            no_tags = re.sub(r"<.*?>", "\n", no_ws)
        except Exception as e:
            self.logger.exception(e)
            return
        # Split into a nice list
        list_report = re.split(r"\n+", no_tags)

        # This is the itertools.groupby method, not the pandas groupby. It works differently!
        # The list is split each time the lambda function is True. This allows for unique keys to
        # be creates where the `OperatingPeriod` is the key
        unique_keys = list()
        groups = list()
        for k, g in groupby(list_report, lambda x: "OperatingPeriod" in x):
            unique_keys.append(k)
            groups.append(list(g))

        # Create a dict object out of the iterrated report
        report_dict = {}
        for idx, (key, group) in enumerate(zip(unique_keys, groups)):
            # if key is True means that the element of groups[idx][0] has 'OperatingPeriod' in the string.
            # The 'OperatingPeriod' becomes the key for the remaining fields of the report
            if key is True:
                report_dict[groups[idx][0]] = groups[idx + 1]

        prod_report = ProductionReport()
        prod_report["county"] = row["county"]
        prod_report["township"] = row["township"]
        prod_report["well_name"] = row["well_name"]
        prod_report["production_report"] = report_dict
        return prod_report

    def parse_production_report_table(self, response):
        return response.xpath('//*[@id="proData"]')

    def get_county_names(self, response):
        # ['+ Allegheny', '+ Armstrong', ...]
        names = response.xpath('//a[starts-with(@id, "munilink")]/text()').extract()
        return [name.replace("+", "").replace(" ", "") for name in names]

    def get_county_ids(self, response):
        ids = response.xpath('//a[starts-with(@id, "munilink")]/@id').extract()
        parsed_ids = [full_id.split("_")[-1] for full_id in ids]
        return [f"munis_{parsed_id}" for parsed_id in parsed_ids]

    def get_county_data_from_count_id(self, response, county_id):
        county_data = response.xpath(f'//div[@id="{county_id}"]')

    def get_townships_by_county_id(self, response, county_id):
        return [x.replace("+ ", "") for x in response.xpath(f'//div[@id="{county_id}"]/a/text()').extract()]

    def get_townships_link_by_county_id(self, response, county_id):
        link_ids = response.xpath(f'//div[@id="{county_id}"]/a/@id').extract()
        return [link.split("_")[-1] for link in link_ids]

    def get_table_header(self, response, permit_link):
        headers = response.xpath(f'//div[@id="permits_{permit_link}"]/table/tbody/tr/th/text()').extract()
        return [header.strip() for header in headers]

    def get_table_rows(self, response, permit_link):
        rows = response.xpath(
            f'//div[@id="permits_{permit_link}"]/table/tbody/tr[starts-with(@class, "record_book_row")]'
        )
        parsed_rows = list()
        for row in rows:
            # This is a selector. Need to get the text of <td>
            # well name
            well_name = row.xpath("td[1]/b/text()").extract()[0].strip()
            # royalty
            royalty = row.xpath("td[2]/nobr/text()").extract()[0].strip()
            # market value
            market_value = row.xpath("td[3]/nobr/text()").extract()[0].strip()
            # mcf
            mcf = row.xpath("td[4]/nobr/text()").extract()[0].strip()
            # link to well_report
            link = row.xpath("td[5]/span/a/@href").extract()[0].strip()
            # date started
            date_start = row.xpath("td[6]/span/text()").extract()[0].strip()
            # permit_number
            permit_number = row.xpath("td[7]/span/text()").extract()[0].strip()

            row_data = {
                "well_name": well_name,
                "royalty": royalty,
                "market_value": market_value,
                "mcf": mcf,
                "link": link,
                "date_start": date_start,
                "permit_number": permit_number,
                "permit_link": permit_link,
            }
            parsed_rows.append(row_data)
        return parsed_rows
