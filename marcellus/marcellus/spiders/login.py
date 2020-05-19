import scrapy
import scrapy_splash
from scrapy.http import FormRequest
from scrapy_splash import SplashFormRequest


class Login(scrapy.Spider):
    name = "login"

    LOGIN_URL = "http://www.marcellusgas.org/login.php"
    start_urls = [LOGIN_URL]

    def parse(self, response):
        # If you need a CSRF token, do it first
        return FormRequest.from_response(
            response,
            callback=self.start_scraping,
            formdata={
                "action": "login",
                "redirect": "pro_update.php",
                "EMAIL": "tjl44@pitt.edu",
                "PASSWORD": "Tyler44",
            },
        )

    def start_scraping(self, response):
        yield scrapy_splash.SplashRequest(url=response.url, callback=self.parse_production_report, args={"wait": 2.0})
        print("HERE IS THE URL: ", response.url)

    def parse_production_report(self, response):
        with open("production_report_with_login.html", "wb") as fout:
            fout.write(response.body)
