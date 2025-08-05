import unittest
import tempfile
import os
import sys
import json
import shutil
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# The sys.path modification ensures the parent directory is in the import path.

from py_de_id import (
    randomize,
    base_dir,
    config,
    deidentify_fhir_resource,
    deliver_clone,
    clone_bundle,
    process_request,
    Deidentifier,
)


class TestPyDeId(unittest.TestCase):
    def setUp(self):
        # Setup a temp directory and config
        self.test_dir = "./input"  # tempfile.mkdtemp()
        os.makedirs(self.test_dir, exist_ok=True)
        base_dir = self.test_dir
        config = {
            "*": [],
            "Patient": [
                {"field": "name", "action": "erase"},
                {
                    "field": "birthDate",
                    "action": "randomize",
                    "params": {"min": 1, "max": 2},
                },
                {"field": "gender", "action": "replace", "params": "unknown"},
            ],
        }

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    @patch("py_de_id.pydeid.cherrypy")
    def test_randomize_date(self, mock_cherrypy):
        params = {"min": 1, "max": 2}
        result = randomize("birthDate", "2020-01-01", params)
        self.assertIsInstance(result, str)
        self.assertRegex(str(result), r"\d{4}-\d{2}-\d{2}")

    def test_randomize_float(self):
        params = {"max": 2}
        result = randomize("weight", 70.5, params)
        self.assertIsInstance(result, float)

    def test_randomize_int(self):
        params = {"min": 1, "max": 2}
        result = randomize("age", 30, params)
        self.assertIsInstance(result, int)

    def test_randomize_string(self):
        params = {"length": 5}
        result = randomize("name", "abcde", params)
        self.assertIsInstance(result, str)
        self.assertEqual(len(str(result)), 5)

    @patch("py_de_id.pydeid.cherrypy")
    def test_deidentify_fhir_resource(self, mock_cherrypy):
        resource = {
            "resourceType": "Patient",
            "meta": {},
            "name": [{"family": "python", "given": ["py-de-id"]}],
            "birthDate": "2020-01-01",
            "gender": "male",
        }
        result = deidentify_fhir_resource(resource)
        self.assertNotIn("meta", result)
        self.assertIn("name", result)
        self.assertEqual(result["gender"], "male")
        self.assertIn("birthDate", result)

    @patch("py_de_id.pydeid.requests.post")
    @patch("py_de_id.pydeid.cherrypy")
    def test_deliver_clone(self, mock_cherrypy, mock_post):
        transaction_id = "test_tx"
        # Prepare files
        data = {
            "target_token": "token",
            "fhir_target": "http://localhost/fhir/",
        }
        bundle = {
            "entry": [
                {
                    "resource": {"id": "1"},
                    "request": {"method": "POST", "url": "Patient"},
                }
            ]
        }
        os.makedirs(os.path.join(self.test_dir, transaction_id), exist_ok=True)
        with open(f"{self.test_dir}/{transaction_id}.json", "w") as f:
            json.dump(data, f)
        assert os.path.exists(f"{self.test_dir}/{transaction_id}.json")
        with open(f"{self.test_dir}/{transaction_id}/clone.json", "w") as f:
            json.dump(bundle, f)
        assert os.path.exists(f"{self.test_dir}/{transaction_id}/clone.json")
        # Mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "entry": [{"response": {"status": 201, "location": "Patient/1"}}]
        }
        mock_post.return_value = mock_response
        deliver_clone(transaction_id)
        mock_post.assert_called()

    @patch("py_de_id.pydeid.deidentify_fhir_resource")
    @patch("py_de_id.pydeid.deliver_clone")
    @patch("py_de_id.pydeid.cherrypy")
    def test_clone_bundle(self, mock_cherrypy, mock_deliver, mock_deid):
        transaction_id = "test_tx"
        bundle = {
            "link": [],
            "type": "collection",
            "entry": [
                {
                    "resource": {"resourceType": "Patient", "id": "1", "meta": {}},
                    "search": {},
                    "fullUrl": "url",
                }
            ],
        }
        os.makedirs(os.path.join(self.test_dir, transaction_id), exist_ok=True)
        with open(f"{self.test_dir}/{transaction_id}/bundle.json", "w") as f:
            json.dump(bundle, f)
        mock_deid.side_effect = lambda r: r
        clone_bundle(transaction_id, True)
        mock_deliver.assert_called_with(transaction_id)
        # Check clone.json created
        with open(f"{self.test_dir}/{transaction_id}/clone.json") as f:
            clone = json.load(f)
            self.assertEqual(clone["type"], "transaction")
            self.assertIn("entry", clone)

    @patch("py_de_id.pydeid.requests.get")
    @patch("py_de_id.pydeid.clone_bundle")
    @patch("py_de_id.pydeid.cherrypy")
    def test_process_request_success(self, mock_cherrypy, mock_clone, mock_get):
        transaction_id = "test_tx"
        data = {
            "source_token": "token",
            "fhir_source": "http://localhost/fhir",
            "deid": True,
        }
        os.makedirs(os.path.join(self.test_dir, transaction_id), exist_ok=True)
        with open(f"{self.test_dir}/{transaction_id}.json", "w") as f:
            json.dump(data, f)
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"entry":[]}'
        mock_get.return_value = mock_response
        process_request(transaction_id)
        mock_clone.assert_called_with(transaction_id, True)
        # Check bundle.json created
        with open(f"{self.test_dir}/{transaction_id}/bundle.json") as f:
            bundle = json.load(f)
            self.assertIn("entry", bundle)

    # @patch("py_de_id.pydeid.requests.get", side_effect=Exception("fail"))
    # @patch("py_de_id.pydeid.cherrypy")
    # def test_process_request_exception(self, mock_cherrypy, mock_get):
    #     transaction_id = "test2_tx"
    #     data = {
    #         "source_token": "token",
    #         "fhir_source": "http://localhost/fhir",
    #         "deid": True,
    #     }
    #     os.makedirs(os.path.join(self.test_dir, transaction_id), exist_ok=True)
    #     with open(f"{self.test_dir}/{transaction_id}.json", "w") as f:
    #         json.dump(data, f)
    #     process_request(transaction_id)
    #     mock_cherrypy.log.assert_called()

    @patch("py_de_id.pydeid.cherrypy")
    def test_health(self, mock_cherrypy):
        global is_healthy
        deidentifier = Deidentifier()
        # is_healthy = True
        # mock_cherrypy.response.status = None
        # result = deidentifier.health()
        # self.assertEqual(mock_cherrypy.response.status, 204)
        is_healthy = False
        mock_cherrypy.response.status = None
        result = deidentifier.health()
        self.assertEqual(mock_cherrypy.response.status, 500)
        self.assertEqual(result, "There are some issues")

    @patch("py_de_id.pydeid.cherrypy")
    @patch("py_de_id.pydeid.process_request")
    def test_deidentify(self, mock_process, mock_cherrypy):
        deidentifier = Deidentifier()
        mock_cherrypy.request.json = {"transaction_id": "tx", "deid": True}
        mock_cherrypy.log = MagicMock()
        mock_cherrypy.response.status = None
        mock_thread = MagicMock()
        with patch("py_de_id.pydeid.threading.Thread", return_value=mock_thread):
            result = deidentifier.deidentify()
            self.assertIn("message", json.loads(result))
            mock_thread.start.assert_called()

        # Test missing transaction_id
        mock_cherrypy.request.json = {}
        mock_cherrypy.log.warning = MagicMock()
        result = deidentifier.deidentify()
        self.assertIn("missing transaction_id", result)

        # Test exception branch
        with patch("py_de_id.pydeid.threading.Thread", side_effect=Exception("fail")):
            mock_cherrypy.request.json = {"transaction_id": "tx", "deid": True}
            result = deidentifier.deidentify()
            self.assertIn("message", json.loads(result))


if __name__ == "__main__":
    unittest.main()
