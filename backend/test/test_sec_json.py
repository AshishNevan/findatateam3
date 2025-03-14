import unittest
import pandas as pd
import numpy as np
from pathlib import Path
import json
from datetime import date
import tempfile
import shutil
from zipfile import ZipFile
import requests
import copy
from sec_json import (
    transform_to_json,
    process_submission,
    SymbolFinancialsSchema,
    FinancialsDataSchema,
    FinancialElementImportSchema,
)


class TestSECJsonTransformation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test data and directories"""
        # Create temporary directories
        cls.temp_dir = tempfile.mkdtemp()
        cls.data_dir = Path(cls.temp_dir) / "data"
        cls.export_dir = Path(cls.temp_dir) / "exportfiles"
        cls.data_dir.mkdir()
        cls.export_dir.mkdir()

        # Create test data
        cls.create_test_data()

        # Download and prepare real SEC data
        cls.prepare_real_data()

    @classmethod
    def tearDownClass(cls):
        """Clean up temporary files"""
        shutil.rmtree(cls.temp_dir)

    @classmethod
    def prepare_real_data(cls):
        """Check for real SEC data for testing"""
        # Check if manually downloaded data exists
        manual_data_path = Path("./data/2023q4.zip")
        manual_ticker_path = Path("./data/ticker.txt")

        if not manual_data_path.exists():
            print(f"Warning: {manual_data_path} not found")
            # Create a flag file to indicate real data is not available
            with open(cls.data_dir / "no_real_data", "w") as f:
                f.write("Real SEC data file not found")
            return

        if not manual_ticker_path.exists():
            print(f"Warning: {manual_ticker_path} not found")
            # Create a flag file to indicate real data is not available
            with open(cls.data_dir / "no_real_data", "w") as f:
                f.write("Ticker file not found")
            return

        print("Found real SEC data files for testing")

    @classmethod
    def create_test_data(cls):
        """Create test SEC data files"""
        # Create test DataFrames
        num_data = {
            "adsh": ["0000123456-22-000123"] * 3,
            "tag": ["Assets", "Liabilities", "Revenue"],
            "value": [1000000, 500000, 750000],
            "uom": ["USD"] * 3,
        }
        cls.df_num = pd.DataFrame(num_data)

        pre_data = {
            "adsh": ["0000123456-22-000123"] * 3,
            "tag": ["Assets", "Liabilities", "Revenue"],
            "stmt": ["BS", "BS", "IC"],
            "plabel": ["Total Assets", "Total Liabilities", "Total Revenue"],
        }
        cls.df_pre = pd.DataFrame(pre_data)

        sub_data = {
            "adsh": ["0000123456-22-000123"],
            "cik": [123456],
            "name": ["Test Company"],
            "countryma": ["US"],
            "cityma": ["New York"],
            "period": [20220331],
            "fp": ["Q1"],
            "fy": [2022],
        }
        cls.df_sub = pd.DataFrame(sub_data)

        tag_data = {
            "tag": ["Assets", "Liabilities", "Revenue"],
            "doc": ["Total Assets", "Total Liabilities", "Total Revenue"],
        }
        cls.df_tag = pd.DataFrame(tag_data)

        sym_data = {"symbol": ["TEST"], "cik": [123456]}
        cls.df_sym = pd.DataFrame(sym_data)

        # Save test data to files
        with ZipFile(cls.data_dir / "2022q1.zip", "w") as zf:
            with tempfile.NamedTemporaryFile(mode="w") as f:
                cls.df_num.to_csv(f.name, sep="\t", index=False)
                zf.write(f.name, "num.txt")
            with tempfile.NamedTemporaryFile(mode="w") as f:
                cls.df_pre.to_csv(f.name, sep="\t", index=False)
                zf.write(f.name, "pre.txt")
            with tempfile.NamedTemporaryFile(mode="w") as f:
                cls.df_sub.to_csv(f.name, sep="\t", index=False)
                zf.write(f.name, "sub.txt")
            with tempfile.NamedTemporaryFile(mode="w") as f:
                cls.df_tag.to_csv(f.name, sep="\t", index=False)
                zf.write(f.name, "tag.txt")

        cls.df_sym.to_csv(
            cls.data_dir / "ticker.txt", sep="\t", index=False, header=False
        )

    def test_schema_validation(self):
        """Test schema validation for financial data"""
        schema = SymbolFinancialsSchema()
        test_data = {
            "startDate": date(2022, 3, 31).isoformat() + "T00:00:00",
            "endDate": date(2022, 6, 30).isoformat() + "T00:00:00",
            "year": 2022,
            "quarter": "Q1",
            "symbol": "TEST",
            "name": "Test Company",
            "country": "US",
            "city": "New York",
            "data": {"bs": [], "cf": [], "ic": []},
        }
        try:
            result = schema.load(test_data)
            self.assertIsNotNone(result)
        except Exception as e:
            self.fail(f"Schema validation failed: {str(e)}")

    def test_process_submission(self):
        """Test processing of a single submission"""
        submission = self.df_sub.iloc[0].to_dict()
        dfNum_filtered = self.df_num
        dfPre_dict = {
            (row["adsh"], row["tag"]): (row["stmt"], row["plabel"])
            for _, row in self.df_pre.iterrows()
        }
        dfTag_dict = dict(zip(self.df_tag["tag"], self.df_tag["doc"]))

        # Ensure CIK is a string for consistent processing
        symbol_dict = {
            str(cik): str(symbol)
            for cik, symbol in zip(self.df_sym["cik"], self.df_sym["symbol"])
        }

        result = process_submission(
            submission, dfNum_filtered, dfPre_dict, dfTag_dict, symbol_dict
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["symbol"], "TEST")
        self.assertEqual(result["year"], 2022)
        self.assertEqual(result["quarter"], "Q1")
        self.assertEqual(len(result["data"]["bs"]), 2)  # Assets and Liabilities
        self.assertEqual(len(result["data"]["ic"]), 1)  # Revenue

    def test_transform_to_json(self):
        """Test the complete transformation process"""
        try:
            # Create export directory
            export_dir = Path("./exportfiles")
            export_dir.mkdir(exist_ok=True)

            # Create a subdirectory for 2023q4
            quarter_dir = export_dir / "2023q4"
            quarter_dir.mkdir(exist_ok=True)

            # Run transformation using existing files
            result = transform_to_json(2023, 4)

            # Verify results
            self.assertEqual(result, 0)

            # Check if any JSON files were created
            json_files = list(quarter_dir.glob("*.json"))
            self.assertTrue(len(json_files) > 0, "No JSON files were created")

            # Validate output content of first file
            with open(json_files[0]) as f:
                data = json.load(f)
                self.assertTrue(isinstance(data["year"], int))
                self.assertTrue(data["quarter"] in ["Q1", "Q2", "Q3", "Q4", "FY"])
                self.assertTrue(len(data["data"]["bs"]) > 0)

        finally:
            # DO NOT delete any files in the data directory
            pass

    def test_error_handling(self):
        """Test error handling for invalid inputs"""
        # Test with invalid submission data
        invalid_submission = {
            "adsh": "invalid",
            "cik": "invalid",
            "period": "invalid",
            "fp": "invalid",
            "fy": np.nan,
            "name": "Test",
            "countryma": "US",
            "cityma": "NY",
        }

        result = process_submission(invalid_submission, pd.DataFrame(), {}, {}, {})
        self.assertIsNone(result)

    def test_data_validation(self):
        """Test data validation and cleaning"""
        # Test with missing values
        num_data_with_nulls = {
            "adsh": ["0000123456-22-000123"] * 3,
            "tag": ["Assets", None, "Revenue"],
            "value": [1000000, np.nan, 750000],
            "uom": ["USD"] * 3,
        }
        df_num_with_nulls = pd.DataFrame(num_data_with_nulls)

        submission = self.df_sub.iloc[0].to_dict()
        dfPre_dict = {
            (row["adsh"], row["tag"]): (row["stmt"], row["plabel"])
            for _, row in self.df_pre.iterrows()
        }
        dfTag_dict = dict(zip(self.df_tag["tag"], self.df_tag["doc"]))

        # Ensure CIK is a string for consistent processing
        symbol_dict = {
            str(cik): str(symbol)
            for cik, symbol in zip(self.df_sym["cik"], self.df_sym["symbol"])
        }

        result = process_submission(
            submission, df_num_with_nulls, dfPre_dict, dfTag_dict, symbol_dict
        )

        self.assertIsNotNone(result)
        # Should only process valid entries
        self.assertTrue(
            len(result["data"]["bs"])
            + len(result["data"]["cf"])
            + len(result["data"]["ic"])
            < 3
        )

    def test_real_data_processing(self):
        """Test processing with real SEC financial statement data"""
        # Look for any available real data in the data directory
        data_dir = Path("./data")
        real_data_files = list(data_dir.glob("2023*.zip"))
        no_data_flag = self.data_dir / "no_real_data"

        if no_data_flag.exists():
            self.skipTest("Real SEC data not available")

        if not real_data_files:
            self.skipTest("No SEC data files found")

        try:
            # Use the first available data file
            real_data_path = real_data_files[0]
            print(f"\nUsing SEC data file: {real_data_path.name}")

            # Dictionary of companies to test (CIK: Symbol)
            test_companies = {
                "320193": "AAPL",  # Apple
                "789019": "MSFT",  # Microsoft
                "1652044": "GOOG",  # Alphabet
                "1018724": "AMZN",  # Amazon
                "1326801": "META",  # Meta
            }

            results = {}

            # Read all necessary data files once
            with ZipFile(real_data_path) as zf:
                try:
                    # Read SUB file
                    print("Reading submission data...")
                    with zf.open("sub.txt") as f:
                        df_sub = pd.read_table(f, delimiter="\t")
                        df_sub_filtered = df_sub[
                            df_sub["cik"].astype(str).isin(test_companies.keys())
                        ]
                        print(
                            f"Found {len(df_sub_filtered)} submissions for test companies"
                        )
                        print("\nSubmission details:")
                        for _, row in df_sub_filtered.iterrows():
                            print(
                                f"CIK: {row['cik']}, ADSH: {row['adsh']}, Period: {row['period']}, FP: {row['fp']}, FY: {row['fy']}"
                            )

                    if len(df_sub_filtered) == 0:
                        self.skipTest("No submissions found for test companies")

                    # Read NUM file
                    print("\nReading numeric data...")
                    with zf.open("num.txt") as f:
                        df_num = pd.read_table(f, delimiter="\t")
                        df_num_filtered = df_num[
                            df_num["adsh"].isin(df_sub_filtered["adsh"])
                        ]
                        print(
                            f"Found {len(df_num_filtered)} numeric entries for test companies"
                        )

                    # Read PRE file
                    print("\nReading presentation data...")
                    with zf.open("pre.txt") as f:
                        df_pre = pd.read_table(f, delimiter="\t")
                        df_pre_filtered = df_pre[
                            df_pre["adsh"].isin(df_sub_filtered["adsh"])
                        ]
                        print(
                            f"Found {len(df_pre_filtered)} presentation entries for test companies"
                        )

                    # Read TAG file
                    print("\nReading tag data...")
                    with zf.open("tag.txt") as f:
                        df_tag = pd.read_table(f, delimiter="\t")
                        print(f"Found {len(df_tag)} tag entries")

                except Exception as e:
                    self.skipTest(f"Error reading SEC data files: {str(e)}")

            # Read symbols - using case-insensitive comparison
            print("\nReading ticker data...")
            df_sym = pd.read_table(
                data_dir / "ticker.txt",
                delimiter="\t",
                header=None,
                names=["symbol", "cik"],
            )

            # Convert symbols to uppercase for consistent comparison
            df_sym["symbol"] = df_sym["symbol"].str.upper()

            print(f"Read {len(df_sym)} ticker entries")

            # Prepare dictionaries
            dfPre_dict = {
                (row["adsh"], row["tag"]): (row["stmt"], row["plabel"])
                for _, row in df_pre_filtered.iterrows()
            }
            dfTag_dict = dict(zip(df_tag["tag"], df_tag["doc"]))

            # Convert CIK to string and ensure symbols are uppercase
            symbol_dict = {
                str(cik): (
                    str(symbol).upper() if isinstance(symbol, str) else str(symbol)
                )
                for cik, symbol in zip(df_sym["cik"], df_sym["symbol"])
            }

            print("\nDictionary sizes:")
            print(f"PRE dict: {len(dfPre_dict)} entries")
            print(f"TAG dict: {len(dfTag_dict)} entries")
            print(f"Symbol dict: {len(symbol_dict)} entries")

            # Process each company
            print("\nProcessing company data...")
            for _, submission in df_sub_filtered.iterrows():
                print(f"\nProcessing submission for CIK {submission['cik']}...")
                company_num_data = df_num_filtered[
                    df_num_filtered["adsh"] == submission["adsh"]
                ]
                print(
                    f"Found {len(company_num_data)} numeric entries for this submission"
                )

                result = process_submission(
                    submission.to_dict(),
                    company_num_data,
                    dfPre_dict,
                    dfTag_dict,
                    symbol_dict,
                )

                if result:
                    print(f"Successfully processed submission for {result['symbol']}")
                    results[result["symbol"]] = result
                else:
                    print(f"Failed to process submission for CIK {submission['cik']}")

            if not results:
                self.skipTest("No valid results found for any test company")

            # Validate results for each company
            for symbol, data in results.items():
                print(f"\nValidating {symbol} financial data:")

                # Basic structure validation
                self.assertIn(symbol, [s.upper() for s in test_companies.values()])
                self.assertTrue(isinstance(data["year"], int))
                self.assertTrue(data["quarter"] in ["Q1", "Q2", "Q3", "Q4", "FY"])

                # Financial metrics validation
                bs_data = {
                    item["concept"]: item["value"] for item in data["data"]["bs"]
                }
                ic_data = {
                    item["concept"]: item["value"] for item in data["data"]["ic"]
                }
                cf_data = {
                    item["concept"]: item["value"] for item in data["data"]["cf"]
                }

                # Balance Sheet validation
                print(f"Balance Sheet Items: {len(data['data']['bs'])}")
                if len(data["data"]["bs"]) > 0:
                    self.assertTrue(any("Assets" in concept for concept in bs_data))

                # Income Statement validation
                print(f"Income Statement Items: {len(data['data']['ic'])}")
                if len(data["data"]["ic"]) > 0:
                    self.assertTrue(any("Revenue" in concept for concept in ic_data))

                # Cash Flow validation
                print(f"Cash Flow Items: {len(data['data']['cf'])}")

                # Data consistency checks
                self.assertTrue(
                    all(
                        isinstance(item["value"], (int, float))
                        for item in data["data"]["bs"]
                    )
                )
                self.assertTrue(
                    all(
                        isinstance(item["value"], (int, float))
                        for item in data["data"]["ic"]
                    )
                )
                self.assertTrue(
                    all(
                        isinstance(item["value"], (int, float))
                        for item in data["data"]["cf"]
                    )
                )

        except Exception as e:
            self.fail(f"Real data processing failed: {str(e)}")


if __name__ == "__main__":
    unittest.main()
