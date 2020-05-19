# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import datetime
import re
import pymongo
from scrapy.exceptions import DropItem


class MarcellusPipeline:
    """
    This is the first pipeline each item flows through.
    Items will be processed to remove unnecessary or bad elements. Production report data are extracted for
    a clean structure ready for MongoDB insert in a later pipeline
    """

    def process_item(self, item, spider):
        """
        Default method to be called for each item
        :param item:
        :param spider:
        :return:
        """
        item = self.process_production_report(item)
        return item

    def process_production_report(self, item):
        """
        Structure the report!
        :return:
        """

        # Create a new record set
        records = list()
        for op_period, values in item["production_report"].items():
            labels = values[0::2]
            targets = values[1::2][: len(labels)]
            new_record = {k.replace(":", "").replace(".", "_"): v for k, v in zip(labels, targets)}
            new_record["period"] = op_period
            records.append(new_record)

        cleaned_records = self.clean_production_report(records)
        item["county"] = item["county"].lower()
        item["township"] = item["township"].lower()
        item["well_name"] = item["well_name"].lower().replace(" ", "_")
        item["production_report"] = cleaned_records
        return item

    def clean_production_report(self, records):
        """
        For every field in the report, some clean is required. Most is done with Regex extraction of the desired
        data group.
        :param records:
        :return:
        """
        parsed = list()
        for record in records:
            avg_production = self.clean_average_production(record)
            est_royalites = self.clean_royalities(record)
            operating_days = self.clean_operating_days(record)
            production_company = self.clean_company(record)
            quantity_of_gas = self.clean_quantity_of_gas(record)
            value_of_gas = self.clean_value_of_gas(record)
            crowd_source_atw = self.clean_crowd_source_atw(record)
            period = self.clean_period(record)
            parsed.append(
                dict(
                    avg_production=avg_production,
                    est_royalites=est_royalites,
                    operating_days=operating_days,
                    production_company=production_company,
                    quanitity_of_gas=quantity_of_gas,
                    value_of_gas=value_of_gas,
                    crowd_source_atw=crowd_source_atw,
                    period=period,
                )
            )
        return parsed

    def clean_average_production(self, records):
        original = records.get("AvgProductionPerDay", "")
        match = re.search(r":\$([0-9.,]+)\(", original)
        if match is None:
            return None
        return float(match.group(1).replace(",", ""))

    def clean_royalities(self, records):
        original = records.get("Est_Royalties", "")
        match = re.search(r".*([0-9,.]+)\(", original)
        if match is None:
            return None
        return float(match.group(1).replace(",", ""))

    def clean_operating_days(self, records):
        original = records.get("OperatingDays", "")
        match = re.search(r"([0-9]+)", original)
        if match is None:
            return None
        return int(match.group(1))

    def clean_company(self, records):
        original = records.get("ProductionCompany", "")
        return original.replace(":", "").replace("&amp", "_")

    def clean_quantity_of_gas(self, records):
        original = records.get("QuantityofGas", "")
        match = re.search(r"([0-9,.]+)", original)
        if match is None:
            return None
        return float(match.group(1).replace(",", ""))

    def clean_crowd_source_atw(self, records):
        original = records.get("crowdsourcedATW", "")
        match = re.search(r"([0-9.,]+)\/", original)
        if match is None:
            return None
        return float(match.group(1).replace(",", ""))

    def clean_period(self, records):
        original = records.get("period", "")
        parsed = original.split(":")[-1].split("-")[-1][:7]
        return datetime.datetime.strptime(parsed, "%b%Y").strftime("%Y-%m")

    def clean_value_of_gas(self, records):
        original = records.get("ValueofGas", "")
        match = re.search(r"\$([0-9,.]+)\(", original)
        if match is None:
            return None
        return float(match.group(1).replace(",", ""))


class MongoDBPipeline:
    """
    Dump the data into MongoDB
    """

    def __init__(self):
        connection = pymongo.MongoClient("localhost", 27017)
        db = connection["marcellus"]
        self.collection = db["report.production"]

    def process_item(self, item, spider):
        valid = True
        for data in item:
            if not data:
                valid = False
                raise DropItem("missing {0}".format(data))
            if valid:
                self.collection.insert(dict(item))
            return item
