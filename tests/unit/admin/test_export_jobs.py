from __future__ import annotations

import csv
import io

from app.admin.export_jobs import _build_dahua_csv, _build_mindbody_csv


class TestBuildMindbodyCsv:
    def test_empty_list(self) -> None:
        result = _build_mindbody_csv([])
        reader = csv.DictReader(io.StringIO(result))
        rows = list(reader)
        assert rows == []
        assert "mindbody_id" in reader.fieldnames

    def test_single_client(self) -> None:
        clients = [
            {
                "Id": "100",
                "FirstName": "Alice",
                "LastName": "Smith",
                "Email": "alice@example.com",
                "MobilePhone": "555-1234",
                "HomePhone": "",
                "WorkPhone": "",
                "Status": "Active",
                "Active": True,
                "BirthDate": "1990-01-01",
                "Gender": "Female",
                "CreationDate": "2025-01-01",
            }
        ]
        result = _build_mindbody_csv(clients)
        reader = csv.DictReader(io.StringIO(result))
        rows = list(reader)
        assert len(rows) == 1
        assert rows[0]["mindbody_id"] == "100"
        assert rows[0]["first_name"] == "Alice"
        assert rows[0]["email"] == "alice@example.com"
        assert rows[0]["gender"] == "Female"

    def test_missing_fields_default_to_empty(self) -> None:
        clients = [{"Id": "200"}]
        result = _build_mindbody_csv(clients)
        reader = csv.DictReader(io.StringIO(result))
        rows = list(reader)
        assert rows[0]["first_name"] == ""
        assert rows[0]["email"] == ""

    def test_multiple_clients(self) -> None:
        clients = [
            {"Id": str(i), "FirstName": f"User{i}", "LastName": "L"} for i in range(5)
        ]
        result = _build_mindbody_csv(clients)
        reader = csv.DictReader(io.StringIO(result))
        rows = list(reader)
        assert len(rows) == 5


class TestBuildDahuaCsv:
    def test_empty_list(self) -> None:
        result = _build_dahua_csv([])
        reader = csv.DictReader(io.StringIO(result))
        rows = list(reader)
        assert rows == []
        assert "user_id" in reader.fieldnames

    def test_single_user(self) -> None:
        users = [
            {
                "UserID": "1",
                "CardName": "Alice Smith",
                "CardNo": "MB00000001",
                "CardStatus": "0",
                "CardType": "0",
                "ValidDateStart": "2026-01-01 00:00:00",
                "ValidDateEnd": "2026-12-31 23:59:59",
            }
        ]
        result = _build_dahua_csv(users)
        reader = csv.DictReader(io.StringIO(result))
        rows = list(reader)
        assert len(rows) == 1
        assert rows[0]["user_id"] == "1"
        assert rows[0]["card_name"] == "Alice Smith"
        assert rows[0]["valid_date_end"] == "2026-12-31 23:59:59"

    def test_missing_fields(self) -> None:
        users = [{"UserID": "2"}]
        result = _build_dahua_csv(users)
        reader = csv.DictReader(io.StringIO(result))
        rows = list(reader)
        assert rows[0]["card_name"] == ""
