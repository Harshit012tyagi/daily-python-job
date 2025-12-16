from rapidfuzz import fuzz
from datetime import datetime, timedelta
from collections import defaultdict
from pymongo import MongoClient
from rapidfuzz import fuzz
import uuid



def find_doc():
    mongo_url = "mongodb://gam-user:utiORLKJDSAFH1873akjdsfh@34.93.133.23:27017/adverse_db?authSource=admin"
    client = MongoClient(mongo_url)
    db = client["adverse_db"]
    collection = db["adverse_db"]

    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    docs = list(collection.find(
        {
            "created_date": {"$regex": f"^({today}|{yesterday})"},
            "Delete": "False"
        },
        {
            "Source Name": 1,
            "Negative_Profiles": 1,
            "Web link of news": 1,
            "uuid": 1,
            "Delete": 1
        }
    ))

    print(f"Fetched {len(docs)} docs")
    return docs

yehdoc=find_doc()
print(yehdoc)
split_docs = []

for doc in yehdoc:
    profiles = doc.get("Negative_Profiles", "")
    
    if not profiles:
        continue

    for name in profiles.split("|"):
        name = name.strip()
        if not name:
            continue

        split_docs.append({
            "negative_profile": name,
            "web_link_of_news": doc.get("Web link of news"),
            "source_name": doc.get("Source Name"),
            "uuid": doc.get("uuid")
        })

print("Total documents after split:", len(split_docs))

asc_sorted = sorted(
    split_docs,
    key=lambda x: x["negative_profile"].lower()
)
def build_uuid_groups_exact_adjacent(data):
    groups = {}
    group_count = 1

    current_group_key = f"group_{group_count}"
    groups[current_group_key] = [data[0]["uuid"]]

    for i in range(1, len(data)):
        curr = data[i]
        prev = data[i - 1]

        # EXACT MATCH + different link
        if (
            curr["negative_profile"] == prev["negative_profile"]
            and curr["web_link_of_news"] != prev["web_link_of_news"]
        ):
            groups[current_group_key].append(curr["uuid"])
        else:
            group_count += 1
            current_group_key = f"group_{group_count}"
            groups[current_group_key] = [curr["uuid"]]

    return groups
def filter_groups_min_two(uuid_groups):
    return {
        group: uuids
        for group, uuids in uuid_groups.items()
        if len(uuids) >= 2
    }
def deduplicate_uuid_groups_final(filtered_groups):
    seen = set()
    final_groups = {}
    group_count = 1

    for group_key, uuids in filtered_groups.items():
        unique_key = tuple(sorted(uuids))  # exact identity of group

        if unique_key not in seen:
            seen.add(unique_key)
            final_groups[f"group_{group_count}"] = uuids
            group_count += 1

    return final_groups

def assign_master_id_to_groups(final_groups):
    master_group_map = {}

    for _, uuids in final_groups.items():
        master_id = str(uuid.uuid4())   # new unique ID for this group
        master_group_map[master_id] = uuids

    return master_group_map
import uuid

def assign_master_id_to_groups(final_groups):
    master_group_map = {}

    for _, uuids in final_groups.items():
        master_id = str(uuid.uuid4())   # new unique ID for this group
        master_group_map[master_id] = uuids

    return master_group_map
    
    
    
    
grouped_data = build_uuid_groups_exact_adjacent(asc_sorted)
filtered_groups = filter_groups_min_two(grouped_data)
final_groups = deduplicate_uuid_groups_final(filtered_groups)
master_groups = assign_master_id_to_groups(final_groups)

from pymongo import MongoClient

def add_groupid_only_for_fetched_docs(
    mongo_uri,
    db_name,
    collection_name,
    fetched_docs,
    master_groups
):
    client = MongoClient(mongo_uri)
    collection = client[db_name][collection_name]

    # uuid -> groupid map
    uuid_to_groupid = {}
    for groupid, uuid_list in master_groups.items():
        for u in uuid_list:
            uuid_to_groupid[u] = groupid

    # sirf fetched docs update karo
    for doc in fetched_docs:
        doc_uuid = doc.get("uuid")
        groupid_value = uuid_to_groupid.get(doc_uuid, "")

        collection.update_one(
            {"_id": doc["_id"]},
            {"$set": {"groupid": groupid_value}}
        )

    client.close()


mongo_uri="mongodb://gam-user:utiORLKJDSAFH1873akjdsfh@34.93.133.23:27017/adverse_db?authSource=admin"

add_groupid_only_for_fetched_docs(
    mongo_uri,
    "adverse_db",
    "adverse_db",
    yehdoc,
    master_groups
)


