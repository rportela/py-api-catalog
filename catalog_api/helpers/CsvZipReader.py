from typing import Optional
import pandas as pd
import zipfile


def read_csv_zip_file(
    file, sep: Optional[str] = ",", encoding: Optional[str] = "utf-8"
) -> dict:
    tables: dict = {}
    with zipfile.ZipFile(file, "r") as z:
        for filename in z.namelist():
            if filename.endswith(".csv"):
                with z.open(filename) as f:
                    tables[filename] = pd.read_csv(f, sep=sep, encoding=encoding)
    return tables
