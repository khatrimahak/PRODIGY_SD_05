from concurrent.futures import ThreadPoolExecutor
import requests
import math 
import pandas as pd
import sys
# Define headers and proxies
headers = {
    'authority': 'core.dxpapi.com',
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-US,en;q=0.9,hi;q=0.8',
    'origin': 'https://www.wolseley.co.uk',
    'referer': 'https://www.wolseley.co.uk/',
    'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
}
proxies = {
    "http": "http://1cbb0b114c0bf2846e60__cr.gb:b2b1ac5af48c665c@gw.dataimpulse.com:823",
    "https": "https://1cbb0b114c0bf2846e60__cr.gb:b2b1ac5af48c665c@gw.dataimpulse.com:823",
}

images = set()
# ,pdp_specifications,pdp_images,pdp_pdfs,availabilitycollect,availabilitydelivered,availabilitypremiumdelivery,BADGES,brandlogo,fgascompliant,marketing_symbols,tradeonly,upsells,producttext,buyable,pdp_image_large,bymodelnumber,brand,replacements,shortdescription,deliverymax,deliverymin,mfpartnumber,whatisincluded
# Function to fetch data from website
def fetch(query,start):
    response = requests.get(f'https://core.dxpapi.com/api/v1/core/?&account_id=6722&domain_key=wolseley&request_id=1205668975672&_br_uid_2=uid%3D2142697168097:v%3D15.0:ts%3D1731592176287:hc%3D10&ref_url=www.wolseley.co.uk%2Foffers-promotions%2Ftrade-deals%2F%3Fpage%3D1&url=www.wolseley.co.uk%2Foffers-promotions%2Ftrade-deals%2F%3Fpage%3D1&request_type=search&search_type=category&q={query}&fl=pid,title,description,price,url,brand,thumb_image,pdp_specifications,pdp_pdfs,mfpartnumber,whatisincluded,auxdescription1&rows=200&start={start}', headers=headers)    
    
    return response


def save(filename,text):
    with open(filename,"w",encoding="utf-8") as file:
        file.write(text)

def get_data(query,file_path):
    try:
        start = 0
        response = fetch(query,start)
        
        # Logging status and saving data to a file
        print(response.status_code)
        save(f"json_data/{query}.json",response.text)

        data_fetched = response.json()
        finalData = []
        for i in data_fetched["response"]["docs"]:
            data = {}
            data["pid"] = i["pid"]
            data["Title"] = i.get("title",None)
            data["Brand"] = i.get("brand",None)
            data["Price"] = i.get("price",None)
            data["Description"] = i.get("description",None)
            data["category"] = f"{file_path} -> {i.get("title",None)}"
            try:
                value = data["mfpartnumber"][0]
            except (KeyError, IndexError):
                value = None
            data["mfpartnumber"] = value
            data["whatisincluded"] = i.get("whatisincluded",None)
            data["Image"] = i.get("thumb_image",None)
            try:
                # Extract the list of URLs
                pdf_urls = i["pdp_pdfs"]

                # Add each PDF URL as "Attachment 1", "Attachment 2", etc.
                for i, pdf_url in enumerate(pdf_urls):
                    data[f"Attachment {i+1}"] = pdf_url
                    images.add(pdf_url)
            except:
                pdf_urls = None

            try:
                specifications = i["pdp_specifications"]
                for item in specifications:
                    key, value = item.split(":", 1)  # Split into key and value at the first colon
                    data[key.strip()] = value.strip()  # Remove any extra whitespace

            except:
                specifications = None
            try:
                features_string = i["auxdescription1"][0]
                # Split the features string by the semicolon delimiter
                features_list = features_string.split(";")

                # Add the features as new key-value pairs to the existing dictionary
                for i, feature in enumerate(features_list):
                    data[f"Feature {i+1}"] = feature.strip()
            except:
                features_string = None
            finalData.append(data)


        iteration = math.floor((data_fetched["response"]["numFound"])/200)
        print(data_fetched["response"]["numFound"])
        print(iteration)
        file_path.replace("","")
        file_path.replace("sub_categories/","")
        if iteration>0:
            print("iterations started")
            for i in range(iteration):
                start += 200
                response = fetch(query,start)
        
                # Logging status and saving data to a file
                print(response.status_code)
                save(f"json_data/{query}_{i}.json",response.text)

                data_fetched = response.json()
                for i in data_fetched["response"]["docs"]:
                    data = {}
                    data["pid"] = i["pid"]
                    data["Title"] = i.get("title",None)
                    data["Brand"] = i.get("brand",None)
                    data["Price"] = i.get("price",None)
                    data["Description"] = i.get("description",None)
                    data["category"] = f"{file_path} -> {i.get("title",None)}"
                    try:
                        value = data["mfpartnumber"][0]
                    except (KeyError, IndexError):
                        value = None
                    data["mfpartnumber"] = value
                    data["whatisincluded"] = i.get("whatisincluded",None)
                    data["Image"] = i.get("thumb_image",None)
                    try:
                        # Extract the list of URLs
                        pdf_urls = i["pdp_pdfs"]

                        # Add each PDF URL as "Attachment 1", "Attachment 2", etc.
                        for i, pdf_url in enumerate(pdf_urls):
                            data[f"Attachment {i+1}"] = pdf_url
                            images.add(pdf_url)
                    except:
                        pdf_urls = None

                    try:
                        specifications = i["pdp_specifications"]
                        for item in specifications:
                            key, value = item.split(":", 1)  # Split into key and value at the first colon
                            data[key.strip()] = value.strip()  # Remove any extra whitespace

                    except:
                        specifications = None
                    try:
                        features_string = i["auxdescription1"][0]
                        # Split the features string by the semicolon delimiter
                        features_list = features_string.split(";")

                        # Add the features as new key-value pairs to the existing dictionary
                        for i, feature in enumerate(features_list):
                            data[f"Feature {i+1}"] = feature.strip()
                    except:
                        features_string = None
                    
                    finalData.append(data)
        return finalData
    except Exception as e:  
        print(f"Error in getting data : {e}")      
        with open("error.txt","a") as fi:
            fi.write(f"{query}\n")





import json
import os
import requests

def fetch_and_save(category_name,category_id,current_path):
    try:
        csv_file_path = os.path.join(current_path, f"{category_name}.csv")
        print(f"Fetching CSV for category: {category_name}, ID: {category_id}")
        finalData = get_data(category_id,csv_file_path)
        with open("data.txt","a") as file:
            file.write(f"Fetching CSV for category: {category_name}, ID: {category_id} with len: {len(finalData)}\n")


        # Save the CSV file
        df = pd.DataFrame(finalData)
        df.to_csv(csv_file_path)
        print(f"Saved CSV: {csv_file_path}")
    except:
        with open("error.txt","a") as file:
            file.write(f"{csv_file_path} and {category_name} and {category_id} ")

def process_tree(tree, base_path="",executor=None):
    for category_name, subcategories in tree.items():
        # Construct the path for the current category
        current_path = os.path.join(base_path, category_name)

        # Ensure the directory exists
        os.makedirs(current_path, exist_ok=True)

        # Check if it's a leaf node with a category_Id
        if "category_Id" in subcategories:
            try:
                category_id = subcategories["category_Id"]
                executor.submit(fetch_and_save,category_name,category_id,current_path)

                
                
            except Exception as e:
                print(f"Error in processing tree : {e}")    
                with open("error.txt","a") as fi:
                    fi.write(f"{query}\n")

        # If there are nested subcategories, recurse
        if isinstance(subcategories, dict):
            process_tree(subcategories, current_path,executor)

def main(u):
    
    print("starting to collect data..")
    with open("tree_structure_nested.json", "r") as file:
        tree_structure = json.load(file)


    output_directory = "category_csvs"  # Base directory for saving CSV files
    os.makedirs(output_directory, exist_ok=True)
    with ThreadPoolExecutor(max_workers=50) as executor:  # Adjust max_workers based on your system's capacity
        process_tree(tree_structure, base_path=output_directory,executor=executor)

    print(f"CSV files have been saved under {output_directory}")

if __name__ == '__main__':
    try:
        main("kk")
        for i in images:
            with open("images.txt","a") as file:
                file.write(f"{i}\n")
        print(f"{len(images)} pdf urls saved ")


    except Exception as e:
        print(f"Error in savig csvs: {e}")                
        


        
                    

