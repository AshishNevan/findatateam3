import logging
import sys
from zipfile import ZipFile
import pandas as pd
from pathlib import Path
import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from marshmallow import Schema, fields
from typing import Dict, List
import warnings


class FinancialElementImportSchema(Schema):
    label = fields.String()
    concept = fields.String()
    info = fields.String()
    unit = fields.String()
    value = fields.Int()


class FinancialsDataSchema(Schema):
    bs = fields.List(fields.Nested(FinancialElementImportSchema()))
    cf = fields.List(fields.Nested(FinancialElementImportSchema()))
    ic = fields.List(fields.Nested(FinancialElementImportSchema()))


class SymbolFinancialsSchema(Schema):
    startDate = fields.DateTime()
    endDate = fields.DateTime()
    year = fields.Int()
    quarter = fields.String()
    symbol = fields.String()
    name = fields.String()
    country = fields.String()
    city = fields.String()
    data = fields.Nested(FinancialsDataSchema)


def process_submission(
    submission_data: Dict,
    dfNum_filtered: pd.DataFrame,
    dfPre_dict: Dict,
    dfTag_dict: Dict,
    symbol_dict: Dict,
    logger=None,
) -> Dict | None:
    if logger is None:
        logger = logging.getLogger(__name__)

    try:
        period_start = date.fromisoformat(str(int(submission_data["period"])))

        result = {
            "startDate": period_start.isoformat(),
            "year": (
                int(submission_data["fy"]) if not np.isnan(submission_data["fy"]) else 0
            ),
            "quarter": str(submission_data["fp"]).strip().upper(),
            "name": submission_data["name"],
            "country": submission_data["countryma"],
            "city": submission_data["cityma"],
            "data": {"bs": [], "cf": [], "ic": []},
        }

        quarter_map = {
            "FY": 12,
            "CY": 12,
            "H1": 6,
            "H2": 6,
            "T1": 4,
            "T2": 4,
            "T3": 4,
            "Q1": 3,
            "Q2": 3,
            "Q3": 3,
            "Q4": 3,
        }

        if result["quarter"] not in quarter_map:
            logger.warning(f"Invalid quarter: {result['quarter']}")
            return None

        result["endDate"] = (
            period_start
            + relativedelta(months=+quarter_map[result["quarter"]], days=-1)
        ).isoformat()

        cik = str(submission_data["cik"])

        if cik in symbol_dict:
            symbol = symbol_dict[cik]
            symbol = str(symbol).upper()
            if 1 <= len(symbol) <= 19:
                result["symbol"] = symbol
            else:
                logger.warning(f"Invalid symbol: CIK <{cik}> -> {symbol}")
                return None
        else:
            cik_no_zeros = cik.lstrip("0")
            if cik_no_zeros in symbol_dict:
                symbol = symbol_dict[cik_no_zeros]
                symbol = str(symbol).upper()
                if 1 <= len(symbol) <= 19:
                    result["symbol"] = symbol
                else:
                    logger.warning(
                        f"Invalid symbol: CIKNZ <{cik_no_zeros}> -> <{symbol}>"
                    )
                    return None
            else:
                logger.warning(f"No symbol found for CIK <{cik}> or <{cik_no_zeros}>")
                return None

        adsh = submission_data["adsh"]

        for _, row in dfNum_filtered.iterrows():
            tag = row["tag"]

            if tag in dfTag_dict:
                label = dfTag_dict[tag]
            else:
                continue

            pre_key = (adsh, tag)
            if pre_key in dfPre_dict:
                stmt, plabel = dfPre_dict[pre_key]
            else:
                continue

            if pd.isna(row["value"]):
                continue

            element = {
                "label": label,
                "concept": tag,
                # "info": plabel.replace('"', "'"),
                "info": plabel,
                "unit": row["uom"],
                "value": int(row["value"]),
            }

            if stmt == "BS":
                result["data"]["bs"].append(element)
            elif stmt == "CF":
                result["data"]["cf"].append(element)
            elif stmt == "IC":
                result["data"]["ic"].append(element)

        logger.info(f"Processed submission <{submission_data['adsh']}>")
        return result

    except Exception as e:
        logger.warning(
            f"Error processing submission <{submission_data['adsh']}>: {str(e)}"
        )
        return None


def transform_to_json(year: int, quarter: int, logger=None) -> int:
    """Transform SEC data to JSON format using parallel processing"""
    if logger is None:
        logger = logging.getLogger(__name__)

    dirname = f"{year}q{quarter}"
    base_path = Path("./data")
    out_path = Path("./exportfiles")

    if not out_path.is_dir() or not (out_path / dirname).is_dir():
        out_path.mkdir(exist_ok=True)
        (out_path / dirname).mkdir()

    logger.info(f"Starting transformation for {dirname}...")
    start_time = datetime.now()

    with ZipFile(base_path / f"{dirname}.zip") as myzip:
        dfNum = pd.read_table(
            myzip.open("num.txt"),
            delimiter="\t",
            usecols=["adsh", "tag", "value", "uom"],
            dtype={"value": "float64"},
        )
        dfPre = pd.read_table(
            myzip.open("pre.txt"),
            delimiter="\t",
            usecols=["adsh", "tag", "stmt", "plabel"],
        )
        dfSub = pd.read_table(
            myzip.open("sub.txt"),
            delimiter="\t",
            usecols=[
                "adsh",
                "cik",
                "name",
                "countryma",
                "cityma",
                "period",
                "fp",
                "fy",
            ],
        )
        dfTag = pd.read_table(
            myzip.open("tag.txt"), delimiter="\t", usecols=["tag", "doc"]
        )

    dfSym = pd.read_table(
        base_path / "ticker.txt", delimiter="\t", header=None, names=["symbol", "cik"]
    )

    logger.info("Data loaded, preprocessing...")

    dfNum = dfNum.dropna(subset=["value"])
    dfTag_dict = dict(zip(dfTag["tag"], dfTag["doc"]))
    symbol_dict = {
        str(cik): str(symbol).upper() if isinstance(symbol, str) else str(symbol)
        for cik, symbol in zip(dfSym["cik"].astype(str), dfSym["symbol"])
    }

    dfPre_dict = {
        (row["adsh"], row["tag"]): (row["stmt"], row["plabel"])
        for _, row in dfPre.iterrows()
    }

    logger.info("Preprocessing complete, starting transformation...")

    schema = SymbolFinancialsSchema()
    chunk_size = 5000

    for chunk_start in range(0, len(dfSub), chunk_size):
        chunk_end = min(chunk_start + chunk_size, len(dfSub))
        dfSub_chunk = dfSub.iloc[chunk_start:chunk_end]

        with ProcessPoolExecutor() as executor:
            logger.info(f"Using {executor} workers")
            logger.info(f"Processing chunk <{chunk_start}> - <{chunk_end}>")
            futures = []

            for _, submission in dfSub_chunk.iterrows():
                dfNum_filtered = dfNum[dfNum["adsh"] == submission["adsh"]]
                logger.info(f"Processing submission <{submission['adsh']}>")
                futures.append(
                    executor.submit(
                        process_submission,
                        submission.to_dict(),
                        dfNum_filtered,
                        dfPre_dict,
                        dfTag_dict,
                        symbol_dict,
                    )
                )
            logger.info(f"Processing {len(futures)} submissions")
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    json_str = json.dumps(result)
                    json_str = json_str.replace("\\r", "").replace("\\n", " ")

                    with open(
                        f"{out_path}/{dirname}/{result['symbol']}_{result['quarter']}_{result['year']}.json",
                        "w",
                    ) as f:
                        f.write(json_str)
                    logger.info(f"Processed submission <{result['symbol']}>")
                else:
                    logger.warning(f"Skipping submission <{submission['adsh']}>")
    end_time = datetime.now()
    processing_time = (end_time - start_time).total_seconds()
    logger.info(
        f"Transformation complete. Total processing time: {processing_time:.2f} seconds"
    )

    return 0


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    transform_to_json(year=2024, quarter=4)
