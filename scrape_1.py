# INPUTS
DATABASE_NAME = "Findchildcarewa_Database.db"
BATCH_SIZE = 50 # how many parallel requests
# END OF INPUTS

#CONSTANTS

#Number of retries (Other than the first try)
NO_OF_RETRIES = 2

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

import warnings
warnings.filterwarnings("ignore")


class Findchildcarewa_Scraper:
    def __init__(self, input_databasename, input_batchsize):
        ## check if inputs are good
        self.inputs_are_good = True
        self.is_interrupted = False

        input_checks = [self.check_input('DATABASE_NAME', 'str', input_databasename),
                        self.check_input('BATCH_SIZE', 'positive_int', input_batchsize)]
        if False in input_checks:
            print("Bad inputs, quit!")
            self.inputs_are_good = False
            return

        ## if still here, set inputs
        self.database_name = input_databasename
        self.batch_size = input_batchsize

        ## create database
        if not os.path.exists(self.database_name):
            print("Creating a new database...")
        else:
            print("Database already exists!")

        self.db_conn = sqlite3.connect(self.database_name, check_same_thread=False)
        self.db_cursor = self.db_conn.cursor()
        self.db_cursor.execute("CREATE TABLE IF NOT EXISTS SearchLinks(search_url TEXT NOT NULL PRIMARY KEY, number_of_items INTEGER)")
        self.db_cursor.execute("CREATE TABLE IF NOT EXISTS Listings(url TEXT NOT NULL PRIMARY KEY, search_url TEXT, html BLOB, time_of_scraping TEXT, timestamp REAL)")
        self.db_cursor.execute("CREATE TABLE IF NOT EXISTS FailedListings(url TEXT NOT NULL PRIMARY KEY, search_url TEXT, time_of_scraping TEXT, timestamp REAL)")

        ## other
        self.good_count = 0
        self.LOCK = threading.Lock()

        ## set input links, just one
        self.input_links = [{"search_url":"https://www.findchildcarewa.org/PSS_Search?ft=Child%20Care%20Center;Family%20Child%20Care%20Home;School-age%20Program;Outdoor%20Nature%20Based%20Program&p=DEL%20Licensed;Exempt%20Care;Formerly%20Licensed;Unlawful%20Care"}]
        
        return


    '''
    def read_inputs(self):
        unique_input_links = {}

        try:
            with open(self.input_file, 'r', encoding='utf-8') as f:
                for line in f:
                    potential_link = line.strip()
                    if "//www.findchildcarewa.org/" in potential_link:
                        unique_input_links[potential_link] = ''
        except Exception as input_exc:
            print("An exception while reading inputs:", repr(input_exc))
            unique_input_links = {} # reset

        return [{"search_url":onelink} for onelink in unique_input_links]
    '''
    

    # Gets a list of links to scrape
    def get_a_list_of_links(self):
        '''
        Parses the Search Links and creates the list of Day care URLs to be scraped
        '''
        if self.inputs_are_good == False or self.is_interrupted == True:
            return

        print("Scraping a list of links...")
        for input_item in self.input_links:
            existence_check = self.db_cursor.execute("SELECT EXISTS(SELECT 1 FROM SearchLinks WHERE search_url=?)", (input_item["search_url"],)).fetchone()[0]
            if existence_check == 1:
                continue # already scraped

            # if here, must scrape this link
            try:
                # make the first request to get tokens
                first_req = requests.get(input_item["search_url"], timeout=60, verify=False)
                first_tree = html.fromstring(first_req.text)

                token_el = first_tree.xpath("//script[contains(text(), 'Visualforce.remoting.Manager.add')]")
                full_token_text = token_el[0].text_content()
                token_json = None
                token_json = json.loads( full_token_text[full_token_text.find('{', full_token_text.find('Visualforce.remoting.Manager.add')) : full_token_text.rfind("}")+1] )
                correct_action_index = None
                for action_index, one_action in enumerate(token_json["actions"]["PSS_SearchController"]["ms"]):
                    if one_action["name"] == "getSOSLKeys":
                        correct_action_index = action_index
                        break # don't look further
                    
                # make a search request
                search_req = requests.post("https://www.findchildcarewa.org/apexremote", timeout=60, verify=False,
                                           headers={"Referer":input_item["search_url"]},
                                           json={"action":"PSS_SearchController","method":"getSOSLKeys",
                                                 "data":["","'Child Care Center','Family Child Care Home','School-age Program','Outdoor Nature Based Program'",
                                                         ["DEL Licensed","Exempt Care","Formerly Licensed","Unlawful Care"],[],None,None,None,[]],
                                                 "type":"rpc","tid":2,
                                                 "ctx":{"authorization":token_json["actions"]["PSS_SearchController"]["ms"][correct_action_index]["authorization"],
                                                        "csrf":token_json["actions"]["PSS_SearchController"]["ms"][correct_action_index]["csrf"],
                                                        "ns":token_json["actions"]["PSS_SearchController"]["ms"][correct_action_index]["ns"],
                                                        "ver":token_json["actions"]["PSS_SearchController"]["ms"][correct_action_index]["ver"],
                                                        "vid":token_json["vf"]["vid"]}
                                                 })
                loaded_results_json = json.loads(search_req.text)

                # parse out items
                item_links_to_save = []
                for loaded_result_object in loaded_results_json:
                    for one_item_id in loaded_result_object["result"]:
                        item_links_to_save.append({"url":"https://www.findchildcarewa.org/PSS_Provider?id=" + one_item_id})

                if len(item_links_to_save) != 0: # save it
                    for item_to_save in item_links_to_save:
                        self.db_cursor.execute("INSERT OR IGNORE INTO Listings(url, search_url) VALUES(?,?)", (item_to_save["url"], input_item["search_url"]))
                    self.db_cursor.execute("INSERT INTO SearchLinks(search_url, number_of_items) VALUES(?,?)", (input_item["search_url"], len(item_links_to_save) ))
                    self.db_conn.commit()
                    print("Found", len(item_links_to_save), "items at", input_item["search_url"])
                else:
                    print("Couldn't find any items at", input_item["search_url"])
                
            except KeyboardInterrupt:
                print("Manual interrupt, quit!")
                self.is_interrupted = True
                return
            except Exception as exc:
                print("An exception at", input_item["search_url"], ":", repr(exc))
                continue
            
        return



    def scrape_html(self):
        '''
        Identifies URLs that need scraping
        Parses DB and looks for Centers that dont have HTML data scraped
        '''
        if self.inputs_are_good == False or self.is_interrupted == True:
            return
        
        #Scrape all
        sql_query = f"SELECT url FROM Listings WHERE html IS NULL"

        #Scrap only 10 listings
        # sql_query = f"""
        # SELECT url
        # FROM Listings
        # WHERE html IS NULL
        # LIMIT 10;
        # """ 

        items_to_scrape = [{"url":x[0]} for x in self.db_cursor.execute(sql_query).fetchall()]
        print("Items left to scrape:", len(items_to_scrape))
        random.shuffle(items_to_scrape) # good for testing
        self.scrape_threaded_from_list(items_to_scrape, self.html_thread, "listing")
        return


    def html_thread(self, input_dict):
        '''
        This method gets multi threaded
        Input : Dictionary with data for scraping
            url only for now
        Scrapes the data and stores the raw html 
        '''
        data_to_save = {"html":None}
        for retry in range(NO_OF_RETRIES):
            try:
                r = requests.get(input_dict["url"], timeout=30, verify=False)
                tree = html.fromstring(r.text)

                verificator_el = tree.xpath("//div[contains(@class, 'provider-detail-form')]//label[contains(@class, 'control-label')]")
                if len(verificator_el) != 0:
                    data_to_save["html"] = bz2.compress(pickle.dumps(r.text))
            except Exception as e:
                if(retry < NO_OF_RETRIES):
                    print(f"RETRY{retry}:{input_dict["url"]}")
                    continue
                print(f"FAILED:{e}:{input_dict["url"]}")
                with self.LOCK:
                    current_time_object = datetime.now()
                    current_time = current_time_object.strftime("%d-%B-%Y")
                    sql_query = f"""
                    INSERT OR REPLACE INTO FailedListings (url, search_url, time_of_scraping, timestamp)
                    VALUES ('{input_dict["url"]}', '{input_dict["url"]}', '{current_time}', '{current_time_object.timestamp()}');
                    """
                    self.db_cursor.execute(sql_query)
                    self.db_conn.commit()
                    print("Couldn't scrape for", input_dict["url"])

        # try:
        #     r = requests.get(input_dict["url"], timeout=30, verify=False)
        #     tree = html.fromstring(r.text)

        #     verificator_el = tree.xpath("//div[contains(@class, 'provider-detail-form')]//label[contains(@class, 'control-label')]")
        #     if len(verificator_el) != 0:
        #         data_to_save["html"] = bz2.compress(pickle.dumps(r.text))
        # except Exception as e:
        #     print(f"FAILED:{e}:{input_dict["url"]}")
        #     with self.LOCK:
        #         current_time_object = datetime.now()
        #         current_time = current_time_object.strftime("%d-%B-%Y")
        #         sql_query = f"""
        #         INSERT OR REPLACE INTO FailedListings (url, search_url, time_of_scraping, timestamp)
        #         VALUES ('{input_dict["url"]}', '{input_dict["url"]}', '{current_time}', '{current_time_object.timestamp()}');
        #         """
        #         self.db_cursor.execute(sql_query)
        #         self.db_conn.commit()
        #         print("Couldn't scrape for", input_dict["url"])

        # save if good
        with self.LOCK:
            try:
                if data_to_save["html"] != None: # save it
                    current_time_object = datetime.now()
                    self.db_cursor.execute("UPDATE Listings SET html=?, time_of_scraping=?, timestamp=? WHERE url=?",
                                           (data_to_save["html"], current_time_object.strftime("%d-%B-%Y"), current_time_object.timestamp(), input_dict["url"] ))
                    self.db_conn.commit()
                    self.good_count+=1
                else:
                    #No Data Scraped
                    #TO DO
                    #Add something to db
                    print("No data scraped")
            except:
                pass
            
        return
    
    


    def scrape_threaded_from_list(self, input_list, input_thread_func, input_print_string, max_items=None, batch_size=None):
        '''
        Takes care of multithreading
        Creates list and calls the method to be multithreaded
        '''
        all_thread_items = []
        self.good_count = 0

        if batch_size == None:
            relevant_batch_size = self.batch_size
        else:
            relevant_batch_size = batch_size
            
        for input_index, input_item in enumerate(input_list):
            if type(max_items) == int:
                if input_index == max_items:
                    break # maximum reached
            
            all_thread_items.append(input_item)
            if len(all_thread_items) == relevant_batch_size:
                ## call it
                all_threads = []
                for a_thread_item in all_thread_items:
                    current_thread = threading.Thread(target=input_thread_func, args=(a_thread_item, ))
                    all_threads.append(current_thread)
                    current_thread.start()

                for thr in all_threads:
                    thr.join()
                    
                print("Current", input_print_string, "item number", input_list.index(input_item)+1, "/", len(input_list),
                      "Good requests in this batch:", self.good_count, "/", len(all_thread_items))
                self.good_count = 0
                all_thread_items = []


        if len(all_thread_items) != 0:
            ## call for residuals
            all_threads = []
            for a_thread_item in all_thread_items:
                current_thread = threading.Thread(target=input_thread_func, args=(a_thread_item, ))
                all_threads.append(current_thread)
                current_thread.start()

            for thr in all_threads:
                thr.join()
                
            print("Current", input_print_string, "item number", input_list.index(input_item)+1, "/", len(input_list),
                  "Good requests in this batch:", self.good_count, "/", len(all_thread_items))
            self.good_count = 0
            all_thread_items = []
            
        return
    

    def check_input(self, input_name, input_type, input_value):
        '''
        Input validation
        '''
        input_is_good = True
        if input_type == 'str':
            if type(input_value) != str:
                input_is_good = False
                print(input_name + " should be a string!")
        elif input_type == 'positive_int':
            if type(input_value) != int:
                input_is_good = False
                print(input_name + " should be an integer!")
            else:
                if input_value <= 0:
                    input_is_good = False
                    print(input_name + " should be a positive integer!")
        else:
            print("Unhandled input type: " + input_type)
            
        return input_is_good



if __name__ == '__main__':
    '''
    Main method
    '''
    # Logging start time
    start = time.time()

    scraper_instance = Findchildcarewa_Scraper(DATABASE_NAME, BATCH_SIZE)
    scraper_instance.get_a_list_of_links()
    scraper_instance.scrape_html()

    # Logging end time
    end = time.time()
    elapsed_time = end - start
    print(f"Elapsed time: {elapsed_time:.6f} seconds")
