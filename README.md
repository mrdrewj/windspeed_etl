# windspeed_etl

**ETL pipeline using Prefect to obtain local windspeed data for a smart device.**

## Overview

Automates the extraction, transformation, and loading (ETL) of local windspeed data using Prefect. Designed to support smart devices with accurate windspeed information.

## Features

- Extracts windspeed data from sources
- Cleans and transforms data
- Loads data for smart device use
- Managed and monitored with Prefect

## Prerequisites

- Python 3.7+
- [Prefect](https://www.prefect.io/)

## Installation

```
git clone https://github.com/mrdrewj/windspeed_etl.git
cd windspeed_etl
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Running the Pipeline

1. Start Prefect agent:

```
prefect agent start
```

2. Register and run the flow:

```
python flows/windspeed_flow.py
```

## Project Structure

```
windspeed_etl/
├── flows/               # Prefect flow scripts
├── data/                # Data storage
├── requirements.txt     # Dependencies
└── README.md             # Documentation
```

## License

MIT License
