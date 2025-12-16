from rapidfuzz import fuzz
from datetime import datetime, timedelta
from collections import defaultdict
from pymongo import MongoClient
from rapidfuzz import fuzz
import uuid
import os


def find_doc():
    mongo_url = os.getenv("MONGO_URI")

    if not mongo_url:
        raise RuntimeError("❌ MONGO_URI not set in environment")

    client = MongoClient(mongo_url, serverSelectionTimeoutMS=20000)
    db = client["adverse_db"]
    collection = db["adverse_db"]  # verify this name

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
import os
from pymongo.errors import PyMongoError


def add_groupid_only_for_fetched_docs(
    mongo_uri: str,
    db_name: str,
    collection_name: str,
    fetched_docs: list,
    master_groups: dict
):
    # 🔐 Hard validation
    if not mongo_uri:
        raise RuntimeError("❌ MONGO_URI not set")

    client = MongoClient(
        mongo_uri,
        serverSelectionTimeoutMS=20000,
        connectTimeoutMS=20000
    )

    try:
        # ✅ force connection check
        client.admin.command("ping")

        collection = client[db_name][collection_name]

        # uuid → groupid map
        uuid_to_groupid = {
            u: groupid
            for groupid, uuid_list in master_groups.items()
            for u in uuid_list
        }

        # sirf fetched docs update karo
        for doc in fetched_docs:
            doc_uuid = doc.get("uuid")

            if not doc_uuid:
                continue

            groupid_value = uuid_to_groupid.get(doc_uuid)

            # ❌ empty / missing groupid mat daalo
            if not groupid_value:
                continue

            collection.update_one(
                {"_id": doc["_id"]},
                {"$set": {"groupid": groupid_value}}
            )

    except PyMongoError as e:
        raise RuntimeError(f"MongoDB update failed: {e}")

    finally:
        client.close()

mongo_url = os.getenv("MONGO_URI")
add_groupid_only_for_fetched_docs(
    mongo_url,
    "adverse_db",
    "adverse_db",
    yehdoc,
    master_groups
)


