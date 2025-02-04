# import requests

# headers = {
#     'authority': 'core.dxpapi.com',
#     'accept': 'application/json, text/plain, */*',
#     'accept-language': 'en-US,en;q=0.9,hi;q=0.8',
#     'origin': 'https://www.wolseley.co.uk',
#     'referer': 'https://www.wolseley.co.uk/',
#     'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
#     'sec-ch-ua-mobile': '?0',
#     'sec-ch-ua-platform': '"Windows"',
#     'sec-fetch-dest': 'empty',
#     'sec-fetch-mode': 'cors',
#     'sec-fetch-site': 'cross-site',
#     'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
# }
# start = 0 
# for i in range(8):
#     response = requests.get(f'https://core.dxpapi.com/api/v1/core/??&fq=tree_level:%22%22&account_id=6722&domain_key=wolseley&request_id=3483170873365&_br_uid_2=uid%3D2142697168097:v%3D15.0:ts%3D1731592176287:hc%3D31&ref_url=www.wolseley.co.uk%2Ffind-products%2F&url=www.wolseley.co.uk%2Ffind-products%2F&catalog_name=category_en&request_type=search&search_type=keyword&q=*&fl=item_id,category_id,category_name,num_products,sub_categories,sort_order,tree_level&rows=100&start={start}&sort=sort_order+asc', headers=headers)
#     with open(f"queries_{i}.json","a") as file:
#         file.write(response.text)
#     start+= 100
#     print(start)






import os
import json
import aiohttp
import asyncio

# API Endpoint (replace with the actual endpoint)
API_ENDPOINT = "https://example.com/api/get_csv"

# Load JSON data
with open("query.json", "r") as file:
    data = file.read()

data = json.loads(data)

# Function to build the tree structure
def build_tree(docs):
    # Create a mapping of all categories by ID
    id_to_category = {doc["category_id"]: doc for doc in docs}

    # Initialize the tree for root categories (tree_level == 1)
    tree = {}

    for doc in docs:
        if doc["tree_level"] == 1:
            # Add root-level categories
            tree[doc["category_name"]] = {
                "category_Id": doc["category_id"],
                "sub_categories": {}
            }

    # Populate subcategories recursively
    def add_subcategories(parent, parent_node):
        if "sub_categories" in parent:
            for sub in parent["sub_categories"]:
                sub_name = sub["name"]
                sub_id = sub["category_id"]
                parent_node["sub_categories"][sub_name] = {
                    "category_Id": sub_id,
                    "sub_categories": {}
                }

                # Find deeper subcategories if available
                if sub_id in id_to_category:
                    add_subcategories(id_to_category[sub_id], parent_node["sub_categories"][sub_name])

    for root_name, root_node in tree.items():
        root_id = root_node["category_Id"]
        if root_id in id_to_category:
            add_subcategories(id_to_category[root_id], root_node)

    return tree

# Recursive function to fetch CSVs and save them
async def fetch_and_save(session, category_id, path):
    try:
        async with session.get(f"{API_ENDPOINT}?category_id={category_id}") as response:
            if response.status == 200:
                os.makedirs(path, exist_ok=True)
                file_path = os.path.join(path, f"{category_id}.csv")
                with open(file_path, "wb") as file:
                    file.write(await response.read())
                print(f"Saved: {file_path}")
            else:
                print(f"Failed for {category_id}: {response.status}")
    except Exception as e:
        print(f"Error fetching {category_id}: {e}")

# Recursive function to process the tree
async def process_tree(tree, session, base_path=""):
    tasks = []

    for category_name, info in tree.items():
        current_path = os.path.join(base_path, category_name)
        category_id = info.get("category_Id")

        if category_id:
            # Add task to fetch CSV for this category
            tasks.append(fetch_and_save(session, category_id, current_path))

        # Recurse for subcategories
        if "sub_categories" in info and info["sub_categories"]:
            tasks.extend(await process_tree(info["sub_categories"], session, current_path))

    return tasks

# Main function to build the tree and fetch CSVs
async def main():
    docs = data["response"]["docs"]
    tree = build_tree(docs)

    # Save the tree structure for debugging
    with open("Products/tree_structure_nested.json", "w") as outfile:
        json.dump(tree, outfile, indent=4)

    # Start fetching CSVs
    
# Run the main function
asyncio.run(main())
