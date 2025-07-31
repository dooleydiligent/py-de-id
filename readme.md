# Py-de-id

A python microservice to deidentify fhir resources

- `GET /health` — returns status 200 (healthy) or 400 (unhealthy).
- `POST /deidentify/:id` — The deidentifier - in the first iteration it only clones the input

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
export FLASK_APP=py_de_id.app:create_app
flask run
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
