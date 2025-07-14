# KinsDT/scripts

A collection of Python scripts for data processing, analysis, and automation. Each script serves a specific utility, ranging from data transformation to operational analytics.

## Table of Contents

- [Overview](#overview)
- [Repository Structure](#repository-structure)
- [Getting Started](#getting-started)
- [Scripts Description](#scripts-description)
- [Contributing](#contributing)
- [License](#license)

## Overview

This repository contains various Python scripts developed for tasks such as meter mapping, data loading, operational analysis, and reliability calculations. The scripts are modular and can be adapted or extended for different data engineering and analytics workflows.

## Repository Structure

| Script Name                        | Purpose / Functionality                |
|-------------------------------------|----------------------------------------|
| Meter_mapping_new.py                | Meter mapping logic                    |
| block_wise_qos.py                   | Quality of service analysis by block   |
| dt_name.py                          | Data transformation naming utilities   |
| dtcap.py                            | Data transformation capacity analysis  |
| excel_to_psql.py                    | Excel to PostgreSQL data loader        |
| loading.py                          | Data loading automation                |
| meter_mapping.py                    | Alternate meter mapping logic          |
| meter_mapping__to__meter_info.py    | Mapping meters to meter info           |
| neut_script.py                      | Neutral current analysis               |
| neutral_current.py                  | Neutral current calculations           |
| neutral_current_flag.py             | Neutral current flagging               |
| operational_template.py             | Operational data template generator    |
| operational_template_part2.py       | Extended operational template          |
| operational_template_part3.py       | Further operational template logic     |
| script_area.py                      | Area-based script logic                |
| script_blockwise_pq.py              | Blockwise power quality calculations   |
| script_blockwise_pq_faster.py       | Optimized blockwise PQ calculations    |
| script_daily_ov.py                  | Daily overvoltage analysis             |
| script_daily_qos.py                 | Daily quality of service analysis      |
| script_daily_uv.py                  | Daily undervoltage analysis            |
| script_indices.py                   | Calculation of various indices         |
| script_reliability_2.py             | Reliability analytics                  |
| script_reliability_database.py      | Reliability database operations        |

## Getting Started

### Prerequisites

- Python 3.x
- [requirements.txt](requirements.txt) dependencies (see below)

### Installation

1. **Clone the repository:**
    ```
    git clone https://github.com/KinsDT/scripts.git
    cd scripts
    ```

2. **Install dependencies:**
    ```
    pip install -r requirements.txt
    ```

3. **Run a script:**
    ```
    python script_name.py
    ```
    Replace `script_name.py` with the actual script you want to execute.

> **Note:** Some scripts may require configuration or input files. Refer to comments at the top of each script for details.

## Scripts Description

- **Meter Mapping Scripts:**  
  Scripts like `Meter_mapping_new.py`, `meter_mapping.py`, and `meter_mapping__to__meter_info.py` handle mapping of meter data to relevant metadata or info tables.

- **Operational Templates:**  
  The `operational_template.py` series generates templates for operational analytics, with each part extending the logic.

- **Quality of Service and Power Analysis:**  
  Scripts such as `block_wise_qos.py`, `script_blockwise_pq.py`, and `script_daily_qos.py` analyze power quality and service metrics at various aggregation levels.

- **Reliability and Indices:**  
  `script_reliability_2.py`, `script_reliability_database.py`, and `script_indices.py` focus on reliability calculations and the computation of operational indices.

- **Data Loading and Transformation:**  
  `excel_to_psql.py` and `loading.py` automate the loading of data from Excel to PostgreSQL and other data sources.

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request. For major changes, open an issue first to discuss what you would like to change.

## License

This repository currently does not specify a license. Please contact the repository owner for usage permissions.

---

*For more details on each script, refer to the comments and documentation within the individual `.py` files.*
