# import Gigglyz
# from Gigglyz.src.scrape_1 import DATABASE_NAME
# from Gigglyz.src.scrape_1 import BATCH_SIZE
# from Gigglyz.src.scrape_1 import Findchildcarewa_Scraper
from scrape_1 import DATABASE_NAME
from scrape_1 import BATCH_SIZE
from scrape_1 import Findchildcarewa_Scraper


# install: requests, lxml, openpyxl
import requests
from lxml import html
from openpyxl import Workbook
import os
import sys
import bz2
import sqlite3
import pickle
import json
import pprint
import time
from datetime import datetime
import threading
import random
import re

import warnings
warnings.filterwarnings("ignore")

#Helper file to geocode address
import geocode



class Findchildcarewa_Parser(Findchildcarewa_Scraper):
    def write_data(self):
        if self.inputs_are_good == False or self.is_interrupted == True:
            return
        
        print("Writing data...")
        headers = [{'header': 'License Name', 'type': 'labeled'}, 
                   {'header': 'Languages Spoken', 'type': 'labeled'},
                   {'header': 'Languages of Instruction', 'type': 'labeled'}, 
                   {'header': 'Primary Contact', 'type': 'labeled'},
                   {'header': 'Email', 'type': 'labeled'}, 
                   {'header': 'Website', 'type': 'labeled'}, 
                   {'header': 'Street address 1', 'type': 'basic'}, 
                   {'header': 'Street address 2', 'type': 'basic'},
                   {'header': 'City', 'type': 'basic'}, 
                   {'header': 'State', 'type': 'basic'}, 
                   {'header': 'ZIP', 'type': 'basic'},
                   {'header': 'Hours of Operation - Mon', 'type': 'hours_of_operation'}, 
                   {'header': 'Hours of Operation - Tue', 'type': 'hours_of_operation'}, 
                   {'header': 'Hours of Operation - Wed', 'type': 'hours_of_operation'},
                   {'header': 'Hours of Operation - Thu', 'type': 'hours_of_operation'}, 
                   {'header': 'Hours of Operation - Fri', 'type': 'hours_of_operation'},
                   {'header': 'Hours of Operation - Sat', 'type': 'hours_of_operation'},
                   {'header': 'Hours of Operation - Sun', 'type': 'hours_of_operation'},
                   {'header': 'Ages', 'type': 'labeled'},
                   {'header': 'Food Program Participation', 'type': 'labeled'}, 
                   {'header': 'Facility Type', 'type': 'labeled'},
                   {'header': 'Age Groups of Available Slots', 'type': 'labeled'}, 
                   {'header': 'Provider Status', 'type': 'basic'}, 

                   {'header': 'License Number', 'type': 'labeled'}, 
                   {'header': 'Provider ID', 'type': 'labeled'},                    
                   {'header': 'Provider Address', 'type': 'basic'},                     
                   {'header': 'Initial License Date', 'type': 'labeled'}, 
                   {'header': 'License Type', 'type': 'labeled'}, 
                   {'header': 'License Status', 'type': 'labeled'}, 
                   {'header': 'Licensed Capacity', 'type': 'labeled'},
                   {'header': 'School District', 'type': 'labeled'}, 
                   {'header': 'Subsidy Participation', 'type': 'labeled'},
                   {'header': 'Mailing Address', 'type': 'labeled'}, 
                   {'header': 'Head Start Funding', 'type': 'labeled'}, 
                   {'header': 'Early Head Start Funding', 'type': 'labeled'}, 
                   {'header': 'ECEAP Funding', 'type': 'labeled'},
                   {'header': 'Total Available Slots', 'type': 'labeled'}, 
                   {'header': 'Tribal Information', 'type': 'labeled'}, 
                   {'header': 'Certifications', 'type': 'labeled'},
                   {'header': 'Primary Contact - Full Name', 'type': 'contacts'}, 
                   {'header': 'Primary Contact - Role', 'type': 'contacts'}, 
                   {'header': 'Primary Contact - Email', 'type': 'contacts'},
                   {'header': 'Primary Contact - Phone', 'type': 'contacts'}, 
                   {'header': 'Primary Contact - Start Date', 'type': 'contacts'}, 
                   {'header': 'Primary Licensor - Full Name', 'type': 'contacts'},
                   {'header': 'Primary Licensor - Role', 'type': 'contacts'}, 
                   {'header': 'Primary Licensor - Email', 'type': 'contacts'}, 
                   {'header': 'Primary Licensor - Phone', 'type': 'contacts'},
                   {'header': 'Primary Licensor - Start Date', 'type': 'contacts'}, 
                   {'header': 'URL', 'type': 'basic'}, 
                   {'header': 'Time of Scraping', 'type': 'basic'},
                   
                   #Custom fields not in the scraped data
                   {'header': 'latitude', 'type': 'custom'}, 
                   {'header': 'longitude', 'type': 'custom'}, 
                   ]
        wb = Workbook(write_only=True)
        ws = wb.create_sheet()
        ws.title = 'Sheet1'
        ws.append([headitem["header"] for headitem in headers])
        
        fetcher = self.db_cursor.execute("SELECT * FROM Listings WHERE html IS NOT NULL") # remove limit
        total_count = 0
        for fetched_row in fetcher:
            parsed_data = self.parse_data(fetched_row[0], fetched_row[2], fetched_row[3])
            total_count+=1
            if total_count%1000 == 0:
                print("Items parsed so far:", total_count)

            '''
            pprint.pprint(parsed_data)
            all_headers = []
            for typekey in parsed_data:
                all_headers+=[{"header":key, "type":typekey} for key in parsed_data[typekey]]
            print(all_headers)
            '''

            # write it
            row_to_write = []
            for header_item in headers:
                value_to_write = None
                if header_item["header"] in parsed_data[header_item["type"]]:
                    value_to_write = parsed_data[header_item["type"]][header_item["header"]]

                row_to_write.append(value_to_write)

            ws.append(row_to_write)


        outfile_name = datetime.now().strftime("%d-%B-%Y %H_%M_%S") + " findchildcarewa_data.xlsx"
        wb.save(outfile_name)
        print("Total items scraped:", total_count)
        print("Created output file:", outfile_name)
            
        return


    def parse_data(self, input_url, html_blob, scrapetime):
        data_to_return = {"basic":{"URL":input_url, "Time of Scraping":scrapetime, "Provider Status":None, "Provider Address":None},
                          "labeled":{},
                          "hours_of_operation":{},
                          "contacts":{},
                          "custom":{}}
        
        tree = html.fromstring(pickle.loads(bz2.decompress(html_blob)))
        #verificator_el = tree.xpath("//div[contains(@class, 'provider-detail-form')]//label[contains(@class, 'control-label')]")

        ## parse it out
        maininfo_root_el = tree.xpath("//div[contains(@class, 'provider-detail-panel')]")
        if len(maininfo_root_el) == 1:

            #Get provider Status
            providerstatus_el = maininfo_root_el[0].xpath(".//div[contains(@class, 'form-group')]/label[text()='Provider Status']/following-sibling::div[1]")
            if len(providerstatus_el) != 0:
                data_to_return["basic"]["Provider Status"] = providerstatus_el[0].text_content().strip()

            # get working hours
            working_hour_els = maininfo_root_el[0].xpath(".//div[contains(@class, 'form-group')]/label[text()='Hours of Operation']/following-sibling::ul[1]/li")
            for working_hour_el in working_hour_els:
                working_hour_object = {"day":None, "hours":None}

                workday_el = working_hour_el.xpath("./div[contains(@class, 'hoursOfOperationLabel')]")
                if len(workday_el) != 0:
                    working_hour_object["day"] = workday_el[0].text_content().strip()

                worktime_el = working_hour_el.xpath("./div[contains(@class, 'hoursOfOperationLabel')]/following-sibling::text()[1]")
                if len(worktime_el) != 0:
                    working_hour_object["hours"] = str(worktime_el[0]).strip()

                # add if good
                if working_hour_object["day"] in ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']:
                    data_to_return["hours_of_operation"]["Hours of Operation - " + working_hour_object["day"]] = working_hour_object["hours"]

            # get address from top left
            address_els = maininfo_root_el[0].xpath("./div[contains(@class, 'panel-body')]/div[contains(@class, 'row')][1]/div[1]/p[1]/text()")
            valid_address_parts = []
            for address_el in address_els:
                fixed_address_part = str(address_el).strip()
                if fixed_address_part != "":
                    valid_address_parts.append(fixed_address_part)
            if len(valid_address_parts) != 0:
                data_to_return["basic"]["Provider Address"] = ", ".join(oneadrpart for oneadrpart in valid_address_parts)
                data_to_return["basic"]["Street address 1"], data_to_return["basic"]["Street address 2"], data_to_return["basic"]["City"], data_to_return["basic"]["State"], data_to_return["basic"]["ZIP"] = self.parse_address(
                        data_to_return["basic"]["Provider Address"])

        ## get labeled fields
        check_for_address_status = True
        labeled_field_els = tree.xpath("//div[contains(@class, 'provider-detail-form')]//div[contains(@class, 'form-group')]")

        # Gets all labeled fields except website
        for labeled_field_el in labeled_field_els:
            this_labeled_field = {"header":None, "value":None}

            labeled_header_el = labeled_field_el.xpath("./label[contains(@class, 'control-label')]")
            if len(labeled_header_el) != 0:
                this_labeled_field["header"] = labeled_header_el[0].text_content().strip()
                if this_labeled_field["header"].endswith(":"):
                    this_labeled_field["header"] = this_labeled_field["header"][0:-1].strip()

            labeled_value_el = labeled_field_el.xpath("./label[contains(@class, 'control-label')]/following-sibling::div[1]/p[contains(@class, 'form-control-static')]")
            if len(labeled_value_el) != 0:
                this_labeled_field["value"] = labeled_value_el[0].text_content().strip()

            # add if good
            if this_labeled_field["header"] != None:
                data_to_return["labeled"][this_labeled_field["header"]] = this_labeled_field["value"]
        
        # Extracts website 
        website_raw = tree.xpath("//div[contains(@class, 'provider-detail-form')]//div[contains(@class, 'form-group')]/label[contains(@class, 'control-label')]/following-sibling::div[1]//p[contains(@class, 'form-control-static')]/a/text()")
        # Add if website exists
        if(len(website_raw) > 0):
            data_to_return["labeled"]["Website"] = website_raw[0]


        ## get contact items
        contact_table_el = tree.xpath("//table[@id='ProviderContactsTable']")
        contact_types_added = {}
        if len(contact_table_el) != 0:
            contact_headers = [contheaderel.text_content().strip().lower() for contheaderel in contact_table_el[0].xpath("./thead/tr[1]/th")]
            for contact_row_el in contact_table_el[0].xpath("./tbody/tr"):
                contact_column_els = contact_row_el.xpath("./td")
                if len(contact_column_els) == len(contact_headers) and len(contact_headers) != 0:
                    this_contact_item = {"Full Name":None, "Role":None, "Email":None, "Phone":None, "Start Date":None}

                    for contact_header_index, contact_header_value in enumerate(contact_headers):
                        if contact_header_value.title() in this_contact_item:
                            this_contact_item[contact_header_value.title()] = contact_column_els[contact_header_index].text_content().strip()

                    # add if good
                    if type(this_contact_item["Role"]) == str:
                        this_contact_item["Role"] = this_contact_item["Role"].title()
                        
                    if this_contact_item["Role"] in ["Primary Contact", "Primary Licensor"]: # correct role
                        if this_contact_item["Role"] not in contact_types_added: # not yet added, so add it
                            contact_types_added[this_contact_item["Role"]] = ''
                            for contact_header_to_add in this_contact_item:
                                data_to_return["contacts"][this_contact_item["Role"] + " - " + contact_header_to_add] = this_contact_item[contact_header_to_add]
                    
        ## get geocode latitude and longitude
        address_to_geocode = data_to_return["basic"]["Provider Address"]
        coords = geocode.get_coords(address_to_geocode)
        data_to_return["custom"]["latitude"] = coords["latitude"]
        data_to_return["custom"]["longitude"] = coords["longitude"]


        ## check a bit
        if len(data_to_return["hours_of_operation"]) == 0:
            #print("No hours of operation found at", input_url)
            pass

        if len(data_to_return["labeled"]) == 0:
            print("No labelled fields found at", input_url)

        if len(data_to_return["contacts"]) == 0:
            #print("No contacts found at", input_url)
            pass

        '''
        if check_for_address_status == True:
            for address_key_to_check in ["ZIP", "State"]:
                if address_key_to_check not in data_to_return["basic"]:
                    print("Address key missing:", address_key_to_check, "at", input_url)
                else:
                    if data_to_return["basic"][address_key_to_check] in [None, ""]:
                        print("Address key empty:", address_key_to_check, "at", input_url)
        '''
            
        return data_to_return


    def parse_address(self, input_string):
        street1 = None
        street2 = None
        city = None
        state = None
        zipcode = None

        address_parts = input_string.split(",")
        for adr_part_index, address_part in enumerate(address_parts):
            state_zip_match = re.findall("^([A-Z]{2}[ ]+)([0-9][0-9-]+[0-9])$", address_part.strip())
            if len(state_zip_match) == 1 and adr_part_index == len(address_parts) -1: # found state and zip, figure out city and street
                state = state_zip_match[0][0].strip()
                zipcode = state_zip_match[0][1].strip()

                city_index = adr_part_index - 1
                if city_index >= 0:
                    city = address_parts[city_index].strip()

                street_indexes = [strindex for strindex in range(0, adr_part_index-1, 1)]
                if len(street_indexes) == 1:
                    street1 = address_parts[street_indexes[0]].strip()
                elif len(street_indexes) == 2:
                    street1 = address_parts[street_indexes[0]].strip()
                    street2 = address_parts[street_indexes[1]].strip()
                else:
                    pass
                
                break # look no further

        return street1, street2, city, state, zipcode


if __name__ == '__main__':
    parser_instance = Findchildcarewa_Parser(DATABASE_NAME, BATCH_SIZE)
    parser_instance.write_data()
