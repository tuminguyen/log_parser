# Log Parser 

The repo is created to parse data from: [Kaggle  Global Terrorism Dataset](https://www.kaggle.com/START-UMD/gtd) in CSV file and some particular dataset on [GDELT](https://www.gdeltproject.org/data.html) to the proper format then import to Elasticsearch for further analysis and visualizations.

## Environment
The code has been tested on:
- Ubuntu 20.04
- Python 3.7.9 _(compatible with python 3+)_

## Installation
Install all the libraries in ```requirements.txt```

<ins>**Using pip**<ins>
```
pip install -r requirements.txt
```

<ins>**Using conda**<ins>

```

```

## Usage

<ins>**CSV**<ins>

```
python csv_parser.py -p path_to_csv_file

# Example:
python csv_parser.py -p terrorism.csv
```

For more instruction on using parameters:

```
python csv_parser.py --help
```

<ins>**GDELLT**<ins>

```
python gdelt_parser.py  -s startdate -e endate --station station_list

# Example:
python gdelt_parser.py  -s 20210407 -e 20210409 --station CNN BCCNEWS DW
```

For more instruction on using parameters:

```
python gdelt_parser.py --help
```
