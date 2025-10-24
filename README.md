# 📚 Book Recommendation System (Goodreads + MLOps)

This project builds a machine learning-based book recommendation system using Goodreads data, with an end-to-end MLOps-ready architecture. It includes data processing, model training, recommendation logic, an API for serving, and Docker for containerization.

## 🙋‍♂️ Team Members

- Purva Agarwal  
- Ananya Asthana  
- Karan Goyal  
- Shivam Sah  
- Shivani Sharma  
- Arpita Wagulde

## 🏗️ Project Architecture Overview


## 📄 Data Sources

- **Goodreads Dataset:** Books, ratings, and metadata from the [Goodbooks dataset](https://cseweb.ucsd.edu/~jmcauley/datasets/goodreads.html)
- Additional sources (if any) can be added under `data/external/`

## 🚀 Getting Started

### 1. Clone the repo

```bash
git clone https://github.com/purva-agarwal/goodreads_recommendations.git
cd goodreads_recommendations
```
### 2. Set up Python Environment

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate     # macOS/Linux
# OR
venv\Scripts\activate        # Windows

# Install dependencies
pip install -r requirements.txt

```

### 3. Run Training Pipeline

Set the following environment variables from terminal.
The variable set is only for this instance of terminal and will not affect others.
```bash
export AIRFLOW_HOME=. [point this to the absolute path of config folder of the cloned repository]
```

Request access to gcp credentials [Access credentials will be shared per user basis].

Place the access credentials in config folder as gcp_credentials.json

```bash
airflow standalone
```

**Access the Airflow UI:**
A login password for the admin user will be shown in the terminal or in
config/simple_auth_manager_passwords.json.generated

Open your browser and go to:

http://localhost:8080

Login using the admin credentials

**Add Connection on Airflow UI:**
1. Admin >> Connections
2. Add Connection
3. Connection ID : goodreads_conn ,  Connection Type : Google Cloud
4. Paste the shared GCP access credentials json at Extra Fields JSON

**Run the DAG:**

1. In the Airflow UI, search for goodreads_recommendation_pipeline DAG
2. Click trigger DAG to start execution.


### 4. Start the Recommendation API

#### Locally (for development):

```bash

```

Visit: [http://localhost:8000/docs](http://localhost:8000/docs) to test the API using Swagger UI.

#### Or via Docker:

```bash

```

## 🐳 Using Docker

### Build the Docker Image

```bash

```

### Run the Docker Container

```bash

```

### Access the API

Open in browser:

## ✅ Features

- ✅ Clean and structured Goodreads data pipeline
- ✅ Feature engineering using TF-IDF, embeddings, and more
- ✅ Book recommendation logic (collaborative & content-based filtering)
- ✅ Model training and evaluation
- ✅ Model serving via FastAPI
- ✅ Containerized with Docker for portability and deployment

## 🧪 Testing

Run unit tests using:

```bash

```

Make sure you activate your virtual environment first.

## 📁 Folder Structure

| Folder/File         | Description                                                                 |
|---------------------|-----------------------------------------------------------------------------|
| `notebooks/`        | Jupyter notebooks for exploration and prototyping                           |
| `src/`              | Source code (data processing, feature engineering, modeling, recommendation)|
| `pipeline/`         | ML pipeline orchestration (e.g., Prefect, MLflow)                           |
| `serving/`          | Model API implementation (e.g., FastAPI or Flask)                           |
| `models/`           | Trained models and serialized objects                                       |
| `tests/`            | Unit tests for core components                                              |
| `dockerfiles/`      | Docker images and related files for reproducibility and deployment          |
| `requirements.txt`  | Project dependencies (alternative: Poetry `pyproject.toml`)                 |
| `.dockerignore`     | Files/folders to exclude from Docker build context                          |
| `.gitignore`        | Files/folders to exclude from version control                               |
| `README.md`         | Project overview and usage instructions                                     |

## 📄 License

This project is for educational purpose.

## 🙋‍♀️ Contact

For questions, issues, or contributions, feel free to open a GitHub Issue or Pull Request.

