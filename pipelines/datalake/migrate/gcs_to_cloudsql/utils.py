# -*- coding: utf-8 -*-
import posixpath
import re


def build_restore_plan_for_backup_type(
  files: list[str], bucket_name: str, backup_type: str
) -> list[dict]:
  if backup_type == "vitacare_historic":
    return build_vitacare_historic_restore_plan(files=files, bucket_name=bucket_name)

  if backup_type == "rnds_vaccine":
    return build_rnds_vaccine_restore_plan(files=files, bucket_name=bucket_name)

  raise ValueError(f"Tipo de backup desconhecido: '{backup_type}'")


def build_vitacare_historic_restore_plan(
  files: list[str], bucket_name: str
) -> list[dict]:
  selected_files = {}

  for file in files:
    info = parse_vitacare_historic_filename(file)
    current = selected_files.get(info["cnes"])

    if current is None or (info["date"], info["time"]) > (
      current["date"],
      current["time"],
    ):
      selected_files[info["cnes"]] = {**info, "file": file}

  return [
    {
      "gcs_uri": build_gcs_uri(bucket_name=bucket_name, blob_name=item["file"]),
      "database_name": f"{item['name']}_{item['cnes']}",
    }
    for item in selected_files.values()
  ]


def build_rnds_vaccine_restore_plan(files: list[str], bucket_name: str) -> list[dict]:
  if not files:
    return []

  selected = max(
    (parse_rnds_vaccine_filename(file) for file in files),
    key=lambda item: (item["date"], item["time"]),
  )

  return [
    {
      "gcs_uri": build_gcs_uri(bucket_name=bucket_name, blob_name=selected["file"]),
      "database_name": "rnds_historic",
    }
  ]


def parse_vitacare_historic_filename(file: str) -> dict:
  filename = posixpath.basename(file).lower()
  match = re.match(
    r"^(?P<name>[a-z_]+)_(?P<cnes>[0-9]+)_(?P<date>[0-9]{8})_"
    r"(?P<time>[0-9]{6})(_old)?\.bak$",
    filename,
  )

  if not match:
    raise ValueError(f"Arquivo fora do padrão vitacare_historic: '{file}'")

  return {
    "file": file,
    "name": match.group("name"),
    "cnes": match.group("cnes"),
    "date": match.group("date"),
    "time": match.group("time"),
  }


def parse_rnds_vaccine_filename(file: str) -> dict:
  filename = posixpath.basename(file).lower()
  match = re.match(
    r"^rnds_vaccine_historic_(?P<date>[0-9]{8})_(?P<time>[0-9]{6})\.bak$", filename
  )

  if not match:
    raise ValueError(f"Arquivo fora do padrão rnds_vaccine: '{file}'")

  return {"file": file, "date": match.group("date"), "time": match.group("time")}


def build_gcs_uri(bucket_name: str, blob_name: str) -> str:
  return f"gs://{bucket_name}/{blob_name}"
