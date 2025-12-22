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
result = []                 # ✅ list
uuid_to_index = {}

for groupid, uuid_list in master_groups.items():
    merge_index = None

    for u in uuid_list:
        if u in uuid_to_index:
            merge_index = uuid_to_index[u]
            break

    if merge_index is not None:
        existing_key = list(result[merge_index].keys())[0]
        existing_list = result[merge_index][existing_key]

        for u in uuid_list:
            if u not in existing_list:
                existing_list.append(u)
            uuid_to_index[u] = merge_index
    else:
        result.append({groupid: uuid_list.copy()})
        idx = len(result) - 1
        for u in uuid_list:
            uuid_to_index[u] = idx
master_groups = {
    groupid: uuid_list
    for group in result
    for groupid, uuid_list in group.items()
}


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


#############################################################################Remove -Similarity#####################################################

from pymongo import MongoClient
from datetime import datetime, timedelta
from collections import defaultdict
from datetime import datetime, timedelta
mongo_urls = os.getenv("MONGO_URI")
def delete_last_day_duplicates(db_name, collection_name, mongo_url):
    """
    Deletes duplicate articles from last 1-day data
    based on FULL Negative_Profiles + Source match.
    Keeps only shortest URL per group.
    Deletes using UUID, not ObjectId.
    """

    client = MongoClient(mongo_urls)
    db = client[db_name]
    collection = db[collection_name]

    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    # Fetch only today's + yesterday's docs
    docs = list(collection.find({
        "created_date": {"$regex": f"^({today}|{yesterday})"}
    }))

    group_data = defaultdict(list)

    # Group only these docs
    for doc in docs:
        neg_full = doc.get("Negative_Profiles", "").strip()
        source = doc.get("Source Name", "").strip()

        if not neg_full:
            continue

        key = (neg_full, source)
        group_data[key].append(doc)

    delete_uuids = []

    for (neg_full, source), recs in group_data.items():
        if len(recs) > 1:
            # sort by URL length ? keep shortest
            sorted_rec = sorted(recs, key=lambda x: len(x["Web link of news"]))

            keep = sorted_rec[0]
            deletes = sorted_rec[1:]

            for d in deletes:
                delete_uuids.append(d["uuid"])

    # Perform delete using UUID
    if delete_uuids:
        result = collection.delete_many({"uuid": {"$in": delete_uuids}})

        print("\n======= Deleted UUIDs =======")
        for u in delete_uuids:
            print(u)

        print(f"\nTotal deleted: {result.deleted_count}")
    else:
        print("\nNo duplicates found for last-day data.")


# -----------------------
# ?? How to call the function
# -----------------------

delete_last_day_duplicates(
    db_name="adverse_db",
    collection_name="adverse_db",
    mongo_url=mongo_urls
)



def find_and_delete_subset_negative_links_last_day():

    mongo_url=mongo_urls
    db_name="adverse_db"
    client = MongoClient(mongo_url)
    db = client[db_name]
    collection = db["adverse_db"]
    # ---- Date Strings ----
    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    # ---- Fetch Today + Yesterday Articles ----
    docs = list(collection.find({
        "created_date": {"$regex": f"^({today}|{yesterday})"}
    }, {
        "Source Name": 1,
        "Negative_Profiles": 1,
        "Web link of news": 1,
        "uuid": 1
    }))

    print(f"\nFetched {len(docs)} docs from last 1 day...\n")

    source_group = defaultdict(list)

    # ---- Group by Source ----
    for d in docs:
        src = d.get("Source Name", "").strip()
        neg = d.get("Negative_Profiles", "").strip()

        if not src or not neg:
            continue

        source_group[src].append(d)

    subset_results = []
    uuids_to_delete = []

    # ---- Subset logic ----
    for src, items in source_group.items():
        n = len(items)

        for i in range(n):
            neg1 = set(x.strip().lower() for x in items[i]["Negative_Profiles"].split("|"))

            for j in range(n):
                if i == j:
                    continue

                neg2 = set(x.strip().lower() for x in items[j]["Negative_Profiles"].split("|"))

                # subset match
                if neg1.issubset(neg2):

                    uuid_val = items[i].get("uuid")

                    subset_results.append({
                        "source": src,
                        "subset_negative": items[i]["Negative_Profiles"],
                        "full_negative": items[j]["Negative_Profiles"],
                        "link": items[i]["Web link of news"],
                        "uuid": uuid_val
                    })

                    if uuid_val:
                        uuids_to_delete.append(uuid_val)

    print("\n===== Subset Negative Links Found (Today + Yesterday Only) =====\n")

    if not subset_results:
        print("No subset matches found.")
        return subset_results

    # ---- Print Results ----
    for x in subset_results:
        print(
            f"[{x['source']}]  UUID: {x['uuid']}\n"
            f"  ? SUBSET: {x['subset_negative']}\n"
            f"  ? FULL:   {x['full_negative']}\n"
            f"  ? Link:   {x['link']}\n"
        )

    # ---- PERFORM DELETE ----
    if uuids_to_delete:
        print("\nDeleting following UUIDs from DB:")
        print(uuids_to_delete)

        delete_result = collection.delete_many({"uuid": {"$in": uuids_to_delete}})
        print(f"\n? Deleted {delete_result.deleted_count} documents.\n")

    else:
        print("\nNo UUIDs to delete.\n")

    return subset_results
    
delete_last_day_duplicates(db_name="adverse_db",collection_name="adverse_db",mongo_url=mongo_urls) 
find_and_delete_subset_negative_links_last_day()






