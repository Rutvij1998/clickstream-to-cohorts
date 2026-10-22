# Dataset

## Source

REES46 e-commerce behavior data from a multi category store, published on Kaggle by Michael Kechinov:

https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store

This project uses the October and November 2019 files only:

- `2019-Oct.csv`
- `2019-Nov.csv`

The Kaggle page describes the bundle as about 14.68 GB of CSV. This repository does not vendor those files. `ingestion/download.sh` downloads them into `data/raw/` when `KAGGLE_USERNAME` and `KAGGLE_KEY` are set. `data/raw/` is gitignored.

Columns, in file order:

| Column | Role |
| --- | --- |
| `event_time` | Timestamp, UTC in the raw file |
| `event_type` | `view`, `cart`, `remove_from_cart`, or `purchase` |
| `product_id` | Product identifier |
| `category_id` | Category identifier |
| `category_code` | Dot separated category path, often null |
| `brand` | Brand, often null |
| `price` | Price of the product at event time |
| `user_id` | User identifier |
| `user_session` | Logged session identifier |

## Credit

Please cite the Kaggle dataset page and REES46 when you describe the extract. The store and the event stream are theirs. This repo only models a public extract.

## License split

- Code, tests, dbt models, Airflow DAGs, and Terraform in this repository: MIT. See [LICENSE](LICENSE).
- The REES46 CSV files, and any Parquet or export you build from them: data files remain under the dataset terms set by the original authors. They are not covered by the MIT license.
- `spark/tests/fixtures/events_raw.csv` and `dbt/seeds/events_silver.csv` are synthetic rows written for tests. They are not a sample of the Kaggle files. Do not report their row counts as properties of the REES46 extract.

## What the pipeline drops

The Spark job keeps a row when the timestamp parses, the event type is one of the four values above, `product_id`, `category_id`, `user_id`, and `user_session` are present, and `price` parses as a number that is at least 0. Anything else is counted as rejected (`invalid_event_time`, `invalid_event_type`, `missing_required`, `missing_price`, `negative_price`) and left out of Parquet. Exact duplicates on the business columns are removed. Empty strings in `category_code` and `brand` become null, then `Unknown` in the enriched model.

The run log for each month is `data/logs/2019-Oct.json` and `data/logs/2019-Nov.json`. Those files are local. They are the source for the result placeholders in the README.
