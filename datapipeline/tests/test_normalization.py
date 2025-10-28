import os
import pytest
from unittest.mock import patch, MagicMock
from datapipeline.scripts.normalization import GoodreadsNormalization


@pytest.fixture(autouse=True)
def set_env(monkeypatch):
    """Ensure AIRFLOW_HOME is defined during tests."""
    monkeypatch.setenv("AIRFLOW_HOME", "config/")


@pytest.fixture
def mock_bq_client():
    """Create a mock BigQuery client."""
    mock_client = MagicMock()
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = None
    mock_client.query.return_value = mock_query_job
    mock_client.project = "test_project"
    return mock_client


@pytest.fixture
def normalization_instance(mock_bq_client):
    """Create GoodreadsNormalization instance with mocked BigQuery client."""
    with patch("datapipeline.scripts.normalization.bigquery.Client", return_value=mock_bq_client):
        gn = GoodreadsNormalization()
        gn.logger = MagicMock()
        return gn


def test_initialization(normalization_instance):
    """Ensure initialization sets credentials and attributes correctly."""
    assert normalization_instance.project_id == "test_project"
    assert normalization_instance.dataset_id == "books"
    assert "goodreads_features_cleaned_staging" in normalization_instance.table
    assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"].endswith("gcp_credentials.json")


def test_log_transform_features_success(normalization_instance, mock_bq_client):
    """Ensure log_transform_features executes query successfully."""
    normalization_instance.log_transform_features()

    query_call = mock_bq_client.query.call_args[0][0]
    assert "LN(popularity_score + 1)" in query_call
    assert "description_length" in query_call
    normalization_instance.logger.info.assert_any_call("Applying log transformations to skewed features...")
    mock_bq_client.query.assert_called_once()


def test_log_transform_features_error(normalization_instance, mock_bq_client):
    """Ensure exception in log_transform_features is logged and raised."""
    mock_bq_client.query.side_effect = Exception("Log transform failed")

    with pytest.raises(Exception, match="Log transform failed"):
        normalization_instance.log_transform_features()

    normalization_instance.logger.error.assert_called()


def test_normalize_user_ratings_success(normalization_instance, mock_bq_client):
    """Ensure normalize_user_ratings executes both ALTER and UPDATE queries."""
    normalization_instance.normalize_user_ratings()
    assert mock_bq_client.query.call_count == 2
    normalization_instance.logger.info.assert_any_call("Applying user-centered rating normalization...")


def test_normalize_user_ratings_error(normalization_instance, mock_bq_client):
    """Ensure exception in normalize_user_ratings is logged and raised."""
    mock_bq_client.query.side_effect = Exception("Normalization failed")

    with pytest.raises(Exception, match="Normalization failed"):
        normalization_instance.normalize_user_ratings()

    normalization_instance.logger.error.assert_called()


def test_run_pipeline_success(normalization_instance):
    """Ensure run executes both steps successfully."""
    with patch.object(normalization_instance, "log_transform_features") as mock_log, \
         patch.object(normalization_instance, "normalize_user_ratings") as mock_norm:
        mock_log.return_value = None
        mock_norm.return_value = None

        normalization_instance.run()

        mock_log.assert_called_once()
        mock_norm.assert_called_once()
        normalization_instance.logger.info.assert_any_call("Good Reads Normalization Pipeline")



def test_run_pipeline_error(normalization_instance):
    """Ensure run raises exception if a step fails."""
    with patch.object(normalization_instance, "log_transform_features", side_effect=Exception("Log error")):
        with pytest.raises(Exception, match="Log error"):
            normalization_instance.run()

def test_environment_variables(mock_bq_client):
    """Test environment variable handling."""
    from datapipeline.scripts.normalization import GoodreadsNormalization


    with patch.dict(os.environ, {'AIRFLOW_HOME': '/custom/path'}):
        with patch("datapipeline.scripts.normalization.bigquery.Client", return_value=mock_bq_client):
            gn = GoodreadsNormalization()
            assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == "/custom/path/gcp_credentials.json"


    with patch.dict(os.environ, {}, clear=True):
        with patch("datapipeline.scripts.normalization.bigquery.Client", return_value=mock_bq_client):
            gn = GoodreadsNormalization()
            assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == "./gcp_credentials.json"


def test_main_executes(monkeypatch):
    """Ensure main() runs the pipeline."""
    from datapipeline.scripts import normalization

    mock_run = MagicMock()
    monkeypatch.setattr(normalization.GoodreadsNormalization, "run", mock_run)
    normalization.main()
    mock_run.assert_called_once()
