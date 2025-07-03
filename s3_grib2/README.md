# NOAA PrecipRate S3 Access Tools for TxDOT

## Quick Start (3 Steps)

1. **Unzip this folder** anywhere on your computer
2. **Open Jupyter Notebook** or JupyterLab
3. **Open `noaa_preciprate_access_demo.ipynb`** and run all cells

The notebook handles all setup automatically.

## What's Included

- **`noaa_preciprate_access_demo.ipynb`** - Interactive demo of all access methods
- **`requirements.txt`** - Python dependencies list
- **`code/`** - Current implementation code
- **`README.md`** - This file

## Requirements

- Python 3.7+ (any recent Python installation)
- Internet connection
- Jupyter Notebook (install with `pip install jupyter`)

## Installation Options

### Option A: Automatic Setup
Open the notebook and run the first cell - it installs everything automatically.

### Option B: Manual Installation
```bash
# Install from requirements file
pip install -r requirements.txt

# Or install individual packages
pip install boto3 requests jupyter
```

### Option C: Alternative Package Managers
```bash
# Using conda
conda install boto3 requests jupyter-notebook

# On Ubuntu/Debian Linux
sudo apt install python3-pip
pip3 install -r requirements.txt

# On Mac with Homebrew
brew install python3
pip3 install -r requirements.txt
```

## Automated Setup Features

The notebook automatically handles:

- Python environment detection  
- Required library installation (boto3, requests)  
- Download directory creation  
- S3 connection testing  
- Sample file downloads  
- Multiple access method demonstrations  

## Access Methods Demonstrated

### Primary Methods (Implemented in Notebook)

1. **Python boto3** - Programmatic API access using AWS SDK
2. **Python requests** - Alternative Python HTTP method

### Additional Methods (Documentation Provided)

3. **curl commands** - Copy-paste terminal commands for immediate use
4. **wget commands** - Linux/Mac terminal download commands

## Core Dependencies

The solution requires only four essential Python packages:

- **boto3** - AWS SDK for S3 access
- **requests** - HTTP client for direct downloads
- **jupyter** - Notebook environment
- **notebook** - Jupyter notebook server

All other dependencies are part of Python's standard library.

## Terminal Command Examples

The notebook provides ready-to-use terminal commands for:
- Windows Command Prompt
- Windows PowerShell  
- Mac Terminal
- Linux Terminal

## File Organization

```
s3_grib2/
├── README.md                              # This file
├── requirements.txt                       # Python dependencies (4 packages)
├── noaa_preciprate_access_demo.ipynb     # Interactive demo
├── code/                                 # Sample implementation
│   ├── grib2_processor.py
│   ├── main_processing.py
│   └── main.py
└── grib2_downloads/                      # Auto-created for downloads
```

## Integration Compatibility

This solution is compatible with:
- Any Python environment (3.7+)
- Snowflake external functions
- Amazon EC2 instances
- Local development environments
- CI/CD pipelines

No AWS credentials are required - utilizes public S3 bucket access.

## Technical Support

If you encounter issues:

1. **Verify internet connectivity** - All methods require internet access
2. **Confirm Python version** - Execute `python --version` (requires 3.7+)
3. **Install Jupyter if needed** - Run `pip install jupyter`

## Implementation Guidance

After completing the demonstration:

1. **Select your preferred access method** based on your technical infrastructure
2. **Integrate with your Snowflake/EC2 environment**
3. **Implement automated downloading** using the demonstrated patterns

---

*This package provides complete access to NOAA's MRMS PrecipRate data from Amazon S3. No AWS credentials required.* 