"""Tests for create_users function."""

import pandas as pd
import pytest
from xnat.session import XNATSession

from test_dummy import XnatpyRequestsMocker
from xmigrate.main import get_resource_metadata


@pytest.fixture
def metadata_experiments(xnatpy_mock: XnatpyRequestsMocker):  # noqa: ANN201, D103
    data = [
        {
            "xsiType": "xnat:ctSessionData",
            "xnat:subjectassessordata/id": "UCL_TEST_XNAT_E00240",
            "insert_date": "2026-02-02 14:54:42.62",
            "project": "HitMesoUploadSite_01",
            "ID": "UCL_TEST_XNAT_E00240",
            "label": "0522c0072",
            "insert_user": "mirsg_service",
            "last_modified": "2026-02-02 14:54:42.62",
            "URI": "/data/experiments/UCL_TEST_XNAT_E00240",
        },
        {
            "xsiType": "xnat:petSessionData",
            "xnat:subjectassessordata/id": "UCL_TEST_XNAT_E00238",
            "insert_date": "2026-02-02 14:54:25.02",
            "project": "HitMesoUploadSite_01",
            "ID": "UCL_TEST_XNAT_E00238",
            "label": "0522c0013",
            "insert_user": "mirsg_service",
            "last_modified": "2026-02-02 14:54:25.02",
            "URI": "/data/experiments/UCL_TEST_XNAT_E00238",
        },
    ]

    # Mock with query parameters that match what get_resource_metadata sends
    xnatpy_mock.get("/data/projects/project_1/experiments", complete_qs=False, json={"ResultSet": {"Result": data}})

    for item in data:
        xnatpy_mock.get(f"/data{item['insert_date']}?format=json", json={"ResultSet": {"Result": [item]}})

    return data


@pytest.fixture
def metadata_subjects(xnatpy_mock: XnatpyRequestsMocker):  # noqa: ANN201, D103
    data = [
        {
            "insert_date": "2023-10-13 19:06:13.705",
            "ID": "UCL_TEST_XNAT_S00033",
            "label": "0522c0072",
            "insert_user": "admin",
            "last_modified": "2026-02-02 14:54:44.225298",
            "URI": "/data/subjects/UCL_TEST_XNAT_S00033",
        },
        {
            "insert_date": "2023-11-15 17:16:07.349",
            "ID": "UCL_TEST_XNAT_S00035",
            "label": "0522c0001",
            "insert_user": "admin",
            "last_modified": "2024-07-30 10:15:27.184697",
            "URI": "/data/subjects/UCL_TEST_XNAT_S00035",
        },
    ]

    # Mock with query parameters that match what get_resource_metadata sends
    xnatpy_mock.get("/data/projects/project_1/subjects", complete_qs=False, json={"ResultSet": {"Result": data}})

    for item in data:
        xnatpy_mock.get(f"/data{item['insert_date']}?format=json", json={"ResultSet": {"Result": [item]}})

    return data


def test_get_resource_metadata(
    tmp_path_factory: pytest.TempPathFactory,
    xnatpy_connections: tuple[XNATSession, XNATSession],
    metadata_subjects,  # noqa: ANN001
) -> None:
    """Test that check fails when source has datatypes not on destination."""
    source_conn, _dest_conn = xnatpy_connections
    output_path = tmp_path_factory.mktemp(basename="sub")

    df1 = pd.read_csv("tests/subjects_metadata_test.csv")
    df2 = pd.DataFrame(metadata_subjects)
    compare = df1.compare(df2)
    get_resource_metadata(source_conn, "project_1", "subjects", output_path)
    assert len(compare) == 0


def test_get_resource_metadata_exp(
    tmp_path_factory: pytest.TempPathFactory, xnatpy_connections: tuple[XNATSession, XNATSession], metadata_experiments  # noqa: ANN001
) -> None:
    """Test that check fails when source has datatypes not on destination."""
    source_conn, _dest_conn = xnatpy_connections
    output_path = tmp_path_factory.mktemp(basename="exp")

    df1 = pd.read_csv("tests/experiments_metadata_test.csv")
    df2 = pd.DataFrame(metadata_experiments)
    compare = df1.compare(df2)
    get_resource_metadata(source_conn, "project_1", "experiments", output_path)
    assert len(compare) == 0
