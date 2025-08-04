from datetime import date
import threading
import json
import os
import json
import sys
import requests
import yaml
from uuid import uuid1
from flask import Flask, jsonify, request, send_file
from pathlib import Path
from datetime import datetime, timedelta
import random
import string
import time

# this file's parent directory
PROJECT_DIR = (
    Path(Path(__file__).parent.resolve().absolute()).parent.resolve().absolute()
)

# Global variable to control the periodic job thread
fetch_job_running = False
deid_job_running = False

with open("./assets/config.yaml") as f:
    config = yaml.safe_load(f)

print(f"config is {config}")


# def get_dependency_graph(bundle: list) -> dict:
#     graph = nx.DiGraph()

#     for entry in bundle:
#         if "resource" not in entry or (
#             "id" not in entry["resource"] and "reference" not in entry
#         ):
#             continue

#         if "id" in entry["resource"]:
#             graph.add_node(
#                 f"{entry['resource']['resourceType']}/{entry['resource']['id']}"
#             )
#         if "subject" in entry["resource"]:
#             graph.add_edge(
#                 f"{entry['resource']['resourceType']}/{entry['resource']['id']}",
#                 f"{entry['resource']['subject']['reference']}",
#             )
#         if "target" in entry["resource"] and isinstance(
#             entry["resource"]["target"], list
#         ):
#             for target in entry["resource"]["target"]:
#                 if "reference" in target:
#                     graph.add_edge(
#                         f"{entry['resource']['resourceType']}/{entry['resource']['id']}",
#                         f"{target['reference']}",
#                     )
#     return nx.topological_sort(graph)


def randomize(field_name, old_value, params):
    if "date" in field_name.lower():
        print(f"randomize {field_name} using {old_value} and {params}")
        seed = date.fromisoformat(old_value)
        start_date = seed - timedelta(days=params["min"])
        end_date = seed + timedelta(days=params["max"])
        delta = (end_date - start_date).days
        random_day = random.randint(0, delta)

        return (start_date + timedelta(random_day)).isoformat()
    if isinstance(old_value, float):
        min_val = old_value - params["max"]
        max_val = old_value + params["max"]
        return round(random.uniform(min_val, max_val), 2)
    if isinstance(old_value, int):
        min_val = old_value - abs(params["min"])
        max_val = old_value + params["max"]
        return random.randint(min_val, max_val)

    length = len(old_value)
    if "length" in params and isinstance(params["length"], int):
        length = params["length"]
    new_str = "".join(random.choices(string.printable, k=length))
    return new_str


def deidentify_fhir_resource(resource):
    del resource["meta"]
    if resource["resourceType"] in config:
        print(f"Processing rules for {resource['resourceType']}", flush=True)
        for key in ["*", resource["resourceType"]]:
            for rule in config[key]:
                if rule["field"] in resource:
                    # erase the field
                    if rule["action"] == "erase":
                        del resource[rule["field"]]
                    # replace the field
                    elif rule["action"] == "replace":
                        resource[rule["field"]] = rule["params"]
                    # randomize - using the original as the seed
                    elif rule["action"] == "randomize":
                        old_value = resource[rule["field"]]
                        resource[rule["field"]] = randomize(
                            rule["field"], old_value, rule["params"]
                        )
                    # merge
                    elif rule["action"] == "merge":
                        print(f"Processing merge rule {rule}", flush=True)
                        input = resource[rule["field"]]
                        for item in rule["params"]:
                            try:
                                print(f"Processing param: {item}", flush=True)
                                new_rule = item.replace("%input%", "input")
                                input = eval(new_rule)
                            except Exception as e:
                                print(f"Exception while processing: {e}")
                        resource[rule["field"]] = input
                    else:
                        print(
                            f"Unknown rule.action {rule}", flush=True, file=sys.stderr
                        )
    return resource


def replace_reference(obj, old_ref, new_ref):
    if "reference" in obj and "/" in obj["reference"]:
        ref = obj["reference"]
        if ref == old_ref:
            obj["reference"] = new_ref
            if "display" in obj:
                obj["display"] = new_ref
    if isinstance(obj, list):
        for item in obj:
            replace_reference(item, old_ref, new_ref)
    else:
        for key, value in obj.items():
            if isinstance(value, dict):
                replace_reference(value, old_ref, new_ref)


def find_entry(entries, resourceType, id):
    match = next(
        (
            entry
            for entry in entries
            if entry["resource"]["resourceType"] == resourceType
            and entry["resource"]["id"] == id
        ),
        None,
    )

    return match


def deliver_clone(clone_dir):
    print(f"Delivering clone {clone_dir}/clone.json")

    _, _, transaction_id = clone_dir.rpartition("/")
    print(f"transactionId {transaction_id}", flush=True)

    # TODO: Almost anything but this
    with open(f"{clone_dir}/../{transaction_id}.json", "r") as file:
        content = file.read()
        data = json.loads(content)

    headers = {
        "Authorization": f"Bearer {data['target_token']}",
        "Content-Type": "application/fhir+json",
    }

    print("Reading the clone", flush=True)
    with open(f"{clone_dir}/clone.json", "r") as file:
        content = file.read()
        bundle = json.loads(content)

    # Only create resources that are specific to this patient (i.e. hasattr 'request')

    newBundleEntry = bundle["entry"]

    batch_size = 15
    print(
        f"There are {len(newBundleEntry)} resources to post.  ({len(newBundleEntry)} // {batch_size}) + {bool(len(newBundleEntry) % batch_size)}"
    )
    num_of_batches = (len(newBundleEntry) // batch_size) + bool(
        divmod(len(newBundleEntry), batch_size)
    )

    batch_no = 0
    while len(newBundleEntry) > 0:
        print(
            f"Sending batch {batch_no+1} of {num_of_batches} resources to {data['fhir_target'][:-1]}",
            flush=True,
        )

        request = {
            "resourceType": "Bundle",
            "type": "transaction",
            "entry": newBundleEntry[:batch_size],
        }

        result = requests.post(
            f"{data['fhir_target'][:-1]}",
            json=request,
            headers=headers,
        )
        print(f"result is {result}", flush=True)
        status = result.status_code

        response = result.json()
        # print(f"response is {response}", flush=True)
        if response:
            if "entry" in response:
                for entry in response["entry"]:
                    if "response" in entry:
                        if entry["response"]["status"] == 201:
                            print(f"Created {entry['response']['location']}")
                        elif entry["response"]["status"] == 429:
                            diagnostics = json.loads(
                                entry["response"]["issue"][0]["diagnostics"]
                            )
                            throttle_time = diagnostics._msBeforeNext
                            print(
                                f"Too many requests. Throttling {throttle_time}",
                                flush=True,
                            )
                            time.sleep(throttle_time / 1000)
                            print(f"Retrying batch {batch_no + 1}")
                            status = 429
        else:
            print(f"Unexpected null result", file=sys.stderr, flush=True)

        if status == 200:
            del newBundleEntry[:15]
            batch_no += 1
        else:
            newBundleEntry[:0] = request["entry"]

    print(f"The clone was delivered")


def oldest_files_first(directory):
    files_with_times = []
    for entry_name in os.listdir(os.path.join("/data", directory)):
        full_path = os.path.join(os.path.join("/data", directory), entry_name)
        if os.path.isfile(full_path):
            if full_path.endswith(".json"):
                modification_time = os.path.getmtime(full_path)
                files_with_times.append((full_path, modification_time))

    # Sort the list of (file_path, modification_time) tuples by modification_time
    sorted_files = sorted(files_with_times, key=lambda x: x[1])

    # Extract only the file paths from the sorted list
    if sorted_files:
        print(f"Returning {sorted_files} files", flush=True)
    return [file_path for file_path, _ in sorted_files]


def clone_bundle(bundle_dir):
    print(f"clone_bundle({bundle_dir})", flush=True)

    def replace_reference(obj, referenceMap):
        if "reference" in obj and "/" in obj["reference"]:
            ref = obj["reference"]

            if ref not in referenceMap:
                # log a warning
                print(f"{ref} not found", flush=True)
            else:
                obj["reference"] = f"{referenceMap[ref]}"
                if "display" in obj:
                    obj["display"] = f"{referenceMap[ref]}"
        for key, value in obj.items():
            if isinstance(value, dict):
                replace_reference(value, referenceMap)

    # _, _, transaction_id = bundle_dir.rpartition("/")

    # with open(f"{bundle_dir}/../{transaction_id}.json", "r") as file:
    #     content = file.read()
    #     data = json.loads(content)

    with open(f"{bundle_dir}/bundle.json", "r") as file:
        bundleData = json.load(file)
        bundleData["type"] = "transaction"
        del bundleData["link"]

        # Create a new map to store the references

        referenceMap = {}

        for entry in bundleData["entry"]:
            if "resource" not in entry or "id" not in entry["resource"]:
                # log a warning
                print(f"There is no resource in entry {entry}", flush=True)
            else:
                entry["resource"] = deidentify_fhir_resource(entry["resource"])

                newResourceId = str(uuid1())

                referenceMap[
                    f'{entry["resource"]["resourceType"]}/{entry["resource"]["id"]}'
                ] = f'{entry["resource"]["resourceType"]}/{newResourceId}'

                referenceMap[f'{entry["resource"]["resourceType"]}/{newResourceId}'] = (
                    f'{entry["resource"]["resourceType"]}/{entry["resource"]["id"]}'
                )

                entry["resource"]["id"] = newResourceId

                if "search" in entry:
                    del entry["search"]
                if "fullUrl" in entry:
                    del entry["fullUrl"]
                # Entries without fullUrl are not specific to this patient
                entry["request"] = {
                    "method": "POST",
                    "url": f"{entry['resource']['resourceType']}",
                }

        for entry in bundleData["entry"]:
            replace_reference(entry, referenceMap)

    with open(f"{bundle_dir}/clone.json", "w") as fileOut:
        json.dump(bundleData, fileOut)
    print(f"Cloned bundle {bundle_dir}/clone.json", flush=True)
    deliver_clone(bundle_dir)


def deid_patient_bundle(event):
    global deid_job_running
    print(f"Searching for work", flush=True)
    lock_path = ""
    lock_dir = ""
    for dirname, _, _ in os.walk("/data/input/"):
        if "." not in dirname and dirname != "/data/input/":
            print(f"Check for lock_file in {dirname}", flush=True)
            lock_path = os.path.join(dirname, "_lock")
            if not os.path.exists(lock_path):
                print(f"Locking {dirname} for work")
                with open(lock_path, "w+", encoding="utf-8") as f:
                    _, _, transaction_id = lock_path.rpartition("/")
                    f.write(transaction_id)
                    lock_dir = dirname
                    print(f"Locked {lock_path}", flush=True)
                    break
    if lock_path == "":
        print("No work available.  Exit thread", flush=True)
        deid_job_running = False
        event.set()
        return
    # process this data
    print(f"Processing bundle {lock_dir}", flush=True)
    clone_bundle(lock_dir)
    deid_job_running = False
    event.set()


def fetch_patient_bundles():
    """Fetch data for waiting requests."""
    global deid_job_running
    global fetch_job_running

    while fetch_job_running:
        # If another request is received while processing then fetch_job_running will toggle to True
        fetch_job_running = False
        for filename in oldest_files_first("input"):
            print(f"deidentifying {filename}", flush=True)
            try:
                work_dir = os.path.join(filename.rpartition(".")[0])
                if not os.path.exists(work_dir):
                    print(f"Preparing {work_dir}", flush=True)
                    os.makedirs(work_dir, exist_ok=True)

                    with open(filename, "r") as file:
                        content = file.read()
                        print(f"loaded {content}", flush=True)
                        data = json.loads(content)
                        # Send the request
                        headers = {
                            "Authorization": f"Bearer {data['source_token']}",
                            "Content-Type": "application/json",
                        }
                        print(
                            f"Getting $everything from {data['fhir_source']}",
                            flush=True,
                        )
                        response = requests.get(data["fhir_source"], headers=headers)
                        print(f"Got {response}", flush=True)
                    # Check the response status code
                    if response.status_code == 200:
                        print("Request successful!", flush=True)
                        # save the headers
                        header_file = os.path.join(work_dir, "headers.json")
                        with open(header_file, "w+", encoding="utf-8") as f:
                            f.write(f"{response.headers}")
                        print(f"Wrote headers to {header_file}", flush=True)

                        # save the data
                        data_file = os.path.join(work_dir, "bundle.json")
                        with open(data_file, "w+", encoding="utf-8") as f:
                            f.write(f"{response.text}")

                        if not deid_job_running:
                            stop_thread = threading.Event()
                            deid_job_running = True
                            deid_thread = threading.Thread(
                                target=deid_patient_bundle, args=(stop_thread,)
                            )
                            deid_thread.daemon = True
                            deid_thread.start()
                            print("Started deid_thread", flush=True)
                    else:
                        print(
                            f"Request failed with status code: {response.status_code}",
                            flush=True,
                        )
                        print("Response Text:", flush=True)
                        print(response.text, flush=True)

            except requests.exceptions.RequestException as e:
                print(f"An error occurred during the request: {e}", flush=True)

        # Exit and go dormant (unless fetch_job_running has changed)


def start_fetch_job():
    """Starts the periodic job in a separate thread."""
    global fetch_job_running
    fetch_job_running = True
    fetch_thread = threading.Thread(target=fetch_patient_bundles)
    fetch_thread.daemon = (
        True  # Allows the main program to exit even if this thread is still running
    )
    fetch_thread.start()
    print("Fetch thread started", flush=True)


def create_app():
    app = Flask(__name__)

    if not "NO_THREADS" in os.environ:
        try:
            os.makedirs("/data/input", exist_ok=True)
        except OSError as e:
            raise TypeError(f"Error creating directory '/data/input': {e}")

        # time.sleep(5)

    @app.route("/hello-world", methods=["GET"])
    def hello_world():
        return "hello world.", 200

    @app.route("/health", methods=["GET"])
    def health():
        # Simple health check logic; always healthy for now
        healthy = True
        return ("", 200) if healthy else ("", 400)

    @app.route("/deidentify/<id>", methods=["POST"])
    def deidentify(id):
        global fetch_job_running
        data = request.get_json()
        # You can log or inspect 'data' here if needed
        if not "transaction_id" in data:
            print("Missing transaction id", flush=True)
            return jsonify({"message": "missing transaction_id"}), 400

        print(f"Create /data/input/${data['transaction_id']}.json", flush=True)
        filepath = f"/data/input/{data['transaction_id']}.json"
        try:
            with open(filepath, "w+", encoding="utf-8") as f:
                json_str = json.dumps(data)
                f.write(json_str)
            print(f"wrote file {data['transaction_id']}.json", flush=True)

            if not fetch_job_running:
                start_fetch_job()

        except Exception as e:
            print("Failed to save the object to disk.", flush=True)

        return jsonify({"message": "OK"}), 200

    @app.route("/favicon.ico", methods=["GET"])
    def favicon():
        return send_file(f"./assets/favicon.ico")

    return app
