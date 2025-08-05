# Py-de-id

A python microservice to deidentify fhir resources - made with [CherryPy](https://cherrypy.dev/)

- `GET /health` — returns status 200 (healthy) or 503 (unhealthy).
- `POST /deidentify/:id` — The deidentifier - in the first iteration it only clones the input
  Payload:

```
{
    transaction_id: "The caller's transaction id",
    fhir_source: "The source url for Patient/:id/$everything",
    source_token: "The source bearer token",
    fhir_target: "The target url - where the clone is to be sent",
    target_token: "The target bearer token",
    id: "The id of the patient to be cloned",
    deid: true | false,
}
```

If deid is false then the clone is not modified during the operation. Otherwise the $everything Bundle is modified according to the instructions in the configuration

## deidentification rules configuration

Deidentification is accomplished globally (i.e. on every resource) and on a per-resource basis using simple configuration.

Configuration recognizes the following actions:

- erase - completely remove a field from the input Bundle
- replace - replace a field with a literal value or values
- randomize - randomize the input value based upon parameters (e.g. replace the birthDate with a random date from -15 to +15 days of the original)
- merge - use list comprehension to selectively modify the input field

See [config.yaml](./assets/config.yaml) for examples

Payload:

```
{
    transaction_id: "The caller's transaction id",
    fhir_source: "The source url for Patient/:id/$everything",
    source_token: "The source bearer token",
    fhir_target: "The target url - where the clone is to be sent",
    target_token: "The target bearer token",
    id: "The id of the patient to be cloned",
    deid: true | false,
}
```

If deid is false then the clone is not modified during the operation. Otherwise the $everything Bundle is modified according to the instructions in the configuration

## deidentification rules configuration

Deidentification is accomplished globally (i.e. on every resource) and on a per-resource basis using simple configuration.

Configuration recognizes the following actions:

- erase - completely remove a field from the input Bundle
- replace - replace a field with a literal value or values
- randomize - randomize the input value based upon parameters (e.g. replace the birthDate with a random date from -15 to +15 days of the original)
- merge - use list comprehension to selectively modify the input field

See [config.yaml](./assets/config.yaml) for examples

## Build Instructions

1. **Install build tools**

```bash
pip install build
```

2. **Build the wheel**

```bash
python -m build
```

The .whl file will be created in the dist/ directory.

## Run the Application

1. **Install dependencies**

```bash
pip install -r requirements.txt
```

2. **Run the app**

```bash
python py_de_id/pydeid.py
```

The app will be available at http://127.0.0.1:5000/.

## Testing Endpoints

- GET /health

```bash
curl -i http://127.0.0.1:5000/health
```

- POST /deidentify/123-xyz

```
curl -X POST http://127.0.0.1:5000/deidentify/123-zyz \
 -H "Content-Type: application/json" \
 -d '{"source_url":"http://consumer:8103/fhir/R4","source_token":"example-token","target_url":"http://customer:8103/fhir/R4","target_token":"example-token","patient_id":"123-xyz"}'
```

## Running Tests

To run the tests, first ensure you have pytest installed:

```bash
pip install pytest
```

Then run:

```bash
pytest
```
