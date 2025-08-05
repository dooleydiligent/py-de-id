from datetime import date
import threading
import json
import os
import json
import sys
import requests
import yaml
from uuid import uuid1
import cherrypy
from pathlib import Path
from datetime import timedelta
import random
import string
import time

# this file's parent directory
PROJECT_DIR = (
    Path(Path(__file__).parent.resolve().absolute()).parent.resolve().absolute()
)

is_healthy = False
if os.path.isdir("/data"):
    base_dir = os.path.join("/data", "input")
else:
    base_dir = os.path.join("/tmp", "input")

with open("./assets/config.yaml") as f:
    config = yaml.safe_load(f)

cherrypy.log(f"config is {config}")


def randomize(field_name, old_value, params):
    if "date" in field_name.lower():
        cherrypy.log(
            f"randomize {field_name} using {old_value} and {params}"
        )
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
        cherrypy.log(
            f"Processing rules for {resource['resourceType']}"
        )
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
                        cherrypy.log(
                            f"Processing merge rule {rule}"
                        )
                        input = resource[rule["field"]]
                        for item in rule["params"]:
                            try:
                                cherrypy.log(
                                    f"Processing param: {item}",
                                   
                                )
                                new_rule = item.replace("%input%", "input")
                                input = eval(new_rule)
                            except Exception as e:
                                cherrypy.log(
                                    f"Exception while processing: {e}"
                                )
                        resource[rule["field"]] = input
                    else:
                        print(f"Unknown rule.action {rule}", file=sys.stderr)
    return resource


def deliver_clone(transaction_id):
    cherrypy.log(
        f"Delivering clone {base_dir}/{transaction_id}/clone.json"
    )

    # TODO: Almost anything but this
    with open(f"{base_dir}/{transaction_id}.json", "r") as file:
        content = file.read()
        data = json.loads(content)

    headers = {
        "Authorization": f"Bearer {data['target_token']}",
        "Content-Type": "application/fhir+json",
    }

    cherrypy.log("Reading the clone")
    with open(f"{base_dir}/{transaction_id}/clone.json", "r") as file:
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
        cherrypy.log(f"result is {result}")
        status = result.status_code

        response = result.json()
        # cherrypy.log(f"response is {response}",  level=cherrypy.log.DEBUG)
        if response:
            if "entry" in response:
                for entry in response["entry"]:
                    if "response" in entry:
                        if entry["response"]["status"] == 201:
                            cherrypy.log(
                                f"Created {entry['response']['location']}"
                            )
                        elif entry["response"]["status"] == 429:
                            diagnostics = json.loads(
                                entry["response"]["issue"][0]["diagnostics"]
                            )
                            throttle_time = diagnostics._msBeforeNext
                            print(
                                f"Too many requests. Throttling {throttle_time}",
                            )
                            time.sleep(throttle_time / 1000)
                            cherrypy.log(
                                f"Retrying batch {batch_no + 1}"
                            )
                            status = 429
        else:
            cherrypy.log(
                f"Unexpected null result", file=sys.stderr
            )

        if status == 200:
            del newBundleEntry[:15]
            batch_no += 1
        else:
            newBundleEntry[:0] = request["entry"]

    cherrypy.log(f"The clone was delivered")
    # Clean up the transaction


def clone_bundle(transaction_id):
    cherrypy.log(f"clone_bundle({transaction_id})")

    def replace_reference(obj, referenceMap):
        if "reference" in obj and "/" in obj["reference"]:
            ref = obj["reference"]

            if ref not in referenceMap:
                # log a warning
                cherrypy.log(f"{ref} not found")
            else:
                obj["reference"] = f"{referenceMap[ref]}"
                if "display" in obj:
                    obj["display"] = f"{referenceMap[ref]}"
        for key, value in obj.items():
            if isinstance(value, dict):
                replace_reference(value, referenceMap)

    with open(f"{base_dir}/{transaction_id}/bundle.json", "r") as file:
        bundleData = json.load(file)
        bundleData["type"] = "transaction"
        del bundleData["link"]

        referenceMap = {}

        for entry in bundleData["entry"]:
            if "resource" not in entry or "id" not in entry["resource"]:
                # log a warning
                cherrypy.log(
                    f"There is no resource in entry {entry}"
                )
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

    with open(f"{base_dir}/{transaction_id}/clone.json", "w") as fileOut:
        json.dump(bundleData, fileOut)
    cherrypy.log(
        f"Cloned bundle {base_dir}/{transaction_id}/clone.json"
    )
    deliver_clone(transaction_id)


def process_request(transaction_id):
    try:
        work_dir = f"{base_dir}/{transaction_id}"
        filename = f"{base_dir}/{transaction_id}.json"

        if not os.path.exists(work_dir):
            cherrypy.log(f"Preparing {work_dir}")
            os.makedirs(work_dir, exist_ok=True)

        with open(filename, "r") as file:
            content = file.read()
            cherrypy.log(f"loaded {content}")
            data = json.loads(content)
            # Get the $everything bundle
            headers = {
                "Authorization": f"Bearer {data['source_token']}",
                "Content-Type": "application/json",
            }
            print(
                f"Getting $everything from {data['fhir_source']}",
            )
            response = requests.get(data["fhir_source"], headers=headers)
            cherrypy.log(f"Got {response}")
        # Check the response status code
        if response.status_code == 200:
            cherrypy.log("Request successful!")

            # save the data
            data_file = os.path.join(work_dir, "bundle.json")
            with open(data_file, "w+", encoding="utf-8") as f:
                f.write(f"{response.text}")

            clone_bundle(transaction_id)
        else:
            print(
                f"Request failed with status code: {response.status_code}",
            )
            cherrypy.log("Response Text: {response.text}")

    except requests.exceptions.RequestException as e:
        cherrypy.log(
            f"An error occurred during the request for job {transaction_id}: {e}"
        )


class Deidentifier(object):
    """Deidentifier"""

    @cherrypy.expose()
    def health(self):
        """Produce status code 200 or 500 depending on health state."""
        if is_healthy:
            cherrypy.response.status = 204
        else:
            cherrypy.response.status = 500
            return "There are some issues"


    @cherrypy.expose
    @cherrypy.tools.json_in()
    @cherrypy.tools.allow(methods=["POST"])
    @cherrypy.tools.json_out()
    def deidentify(self, transaction_id=None):
        global is_healthy
        data = cherrypy.request.json
        if not "transaction_id" in data:
            cherrypy.log.warning("Missing transaction id")
            cherrypy.response.status = 400
            return json.dumps({"message": "missing transaction_id"})

        my_transaction_id = str(uuid1())
        cherrypy.log(
            f"Create {base_dir}/{my_transaction_id}.json"
        )
        filepath = f"{base_dir}/{my_transaction_id}.json"
        try:
            with open(filepath, "w+", encoding="utf-8") as f:
                json_str = json.dumps(data)
                f.write(json_str)
            cherrypy.log(
                f"wrote file {my_transaction_id}.json"
            )

            fetch_thread = threading.Thread(
                target=process_request, args=(my_transaction_id,)
            )
            fetch_thread.daemon = True  # Allows the main program to exit even if this thread is still running
            fetch_thread.start()

        except Exception as e:
            is_healthy = False
            cherrypy.log("Failed to save the object to disk.")

        return json.dumps({"message": "OK", "transaction": my_transaction_id})



if __name__ == "__main__":

    if not "NO_THREADS" in os.environ:
        try:
            os.makedirs(base_dir, exist_ok=True)
        except OSError as e:
            raise TypeError(f"Error creating directory '{base_dir}': {e}")

    thread_pool = 10

    cherrypy.log(f"Setting thread pool to {thread_pool}")

    cherrypy.config.update(
        {
            "server.socket_host": "0.0.0.0",
            "server.socket_port": 5000,
            "server.thread_pool": thread_pool,
            "log.screen": True,
            "log.access_file": "",  # Disable access log file
            "log.error_file": "",  #  Disable error log file
        }
    )

    cherrypy.tree.mount(
        Deidentifier(),
        "/",
        {
        '/health': {
            'tools.trailing_slash.extra': False
        },
        '/deidentify': {
            'tools.json_in.on': True,
            'tools.json_out.on': True,
            'tools.allow.methods': ['POST'],
        },
        "/favicon.ico": {
            "tools.staticfile.on": True,
            "tools.staticdir.root": os.getcwd(),
            "tools.staticfile.filename": "./assets/favicon.ico",
        },
      }
    )

    cherrypy.log("Starting the engine")
    is_healthy = True
    cherrypy.engine.start()
    cherrypy.engine.block()
    cherrypy.log("Engine stopped")
