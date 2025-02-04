import requests
import json
start = 0

headers = {
    "authority": "core.dxpapi.com",
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9,hi;q=0.8",
    "origin": "https://www.wolseley.co.uk",
    "referer": "https://www.wolseley.co.uk/",
    "sec-ch-ua": '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
}
finalData = []
for i in range(10):
    
    url = f'https://core.dxpapi.com/api/v1/core/?&url=https://www.wolseley.co.uk&catalog_name=category_en&request_type=search&search_type=keyword&q=*&fl=item_id,category_id,tree_level,sort_order,category_name,category_icon,seo_url,sub_categories&rows=100&start={start}&fq=tree_level:%22%22&sort=sort_order+asc&request_id=5093051414534&account_id=6722&domain_key=wolseley&_br_uid_2=uid=2142697168097:v=15.0:ts=1731592176287:hc=58'
    response = requests.get(url,headers=headers)
    data = response.json()
    docs = data["response"]["docs"]
    if i==0:
        with open(f"query.json","w") as file:
            file.write(str(response.text))  
    else:
        with open(f"query.json","r") as file:
            query = json.load(file)

            for i in docs:
                query["response"]["docs"].append(i)
        with open("query.json","w") as file:
            file.write(json.dumps(query))  
        
    print("Done......")


    start += 100
    
with open("data.txt","w") as file:
    file.write(str(finalData))