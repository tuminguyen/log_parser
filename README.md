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
conda install --file requirements.txt
```

## Usage

<ins>**CSV**<ins>

**Params**
```
'--path', '-p':
    type=str,
    des`cription='path to data file'
'--dump', '-d':
    default=False,
    type=bool, 
    description='dump or log for Beats fetch or not (True: dump, False: not dump)'
'--output', '-o':
    default='log.json',
    type=str,
    description:'define where to dump log, only use when --dump = True'
```

**Run**

```
python csv_parser.py -p path_to_csv_file -o output_file -d True/False

# Orignal way:
python csv_parser.py -p terrorism.csv

# Dump log for Beats, default to log.json
python csv_parser.py -p terrorism.csv -d True 

# Dump log for Beats to specific file
python csv_parser.py -p terrorism.csv -d True -o output.json 
```

For more instruction on using parameters:

```
python csv_parser.py --help
```

<ins>**GDELT**<ins>

**TV News**
```
python gdelt_parser.py  -s startdate -e enddate --station station_list

# Example:
python gdelt_parser.py  -s 20210407 -e 20210409 --station CNN BCCNEWS DW
```

**Events 2.0**

```
python gdelt_parser.py  -s startdate -e enddate 

# Example 1: set all start + end date
python gdelt_parser.py -s 20210412 -e 20210416

# Example 2: set start date, end to default (now)
python gdelt_parser.py -s 20210416
```

For more instruction on using parameters:

```
python gdelt_parser.py --help
```

Customize your mapping body if you want. You can also use _**"analyzer"**_ for some specific fields.

## References
[GDELT 2.0 Events](http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf)

[GDELT TV News](https://blog.gdeltproject.org/announcing-the-television-news-ngram-datasets-tv-ngram/)