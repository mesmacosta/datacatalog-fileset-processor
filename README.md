# Datacatalog Fileset Processor 

[![CircleCI][1]][2] [![PyPi][5]][6] [![License][7]][7] [![Issues][8]][9]

A package to manage Google Cloud Data Catalog Fileset scripts.

**Disclaimer: This is not an officially supported Google product.**

## Executing in Cloud Shell
````bash
# Set your SERVICE ACCOUNT, for instructions go to 1.3. Auth credentials
# This name is just a suggestion, feel free to name it following your naming conventions
export GOOGLE_APPLICATION_CREDENTIALS=~/datacatalog-fileset-processor-sa.json

# Install datacatalog-fileset-processor
pip3 install datacatalog-fileset-processor --user

# Add to your PATH
export PATH=~/.local/bin:$PATH

# Look for available commands
datacatalog-fileset-processor --help
````

## 1. Environment setup

### 1.1. Python + virtualenv

Using [virtualenv][3] is optional, but strongly recommended unless you use [Docker](#12-docker).

#### 1.1.1. Install Python 3.6+

#### 1.1.2. Get the source code
```bash
git clone https://github.com/mesmacosta/datacatalog-fileset-processor
cd ./datacatalog-fileset-processor
```

_All paths starting with `./` in the next steps are relative to the `datacatalog-fileset-processor`
folder._

#### 1.1.3. Create and activate an isolated Python environment

```bash
pip install --upgrade virtualenv
python3 -m virtualenv --python python3 env
source ./env/bin/activate
```

#### 1.1.4. Install the package

```bash
pip install --upgrade .
```

### 1.2. Docker

Docker may be used as an alternative to run the script. In this case, please disregard the
[Virtualenv](#11-python--virtualenv) setup instructions.

### 1.3. Auth credentials

#### 1.3.1. Create a service account and grant it below roles

- Data Catalog Admin

#### 1.3.2. Download a JSON key and save it as
This name is just a suggestion, feel free to name it following your naming conventions
- `./credentials/datacatalog-fileset-processor-sa.json`

#### 1.3.3. Set the environment variables

_This step may be skipped if you're using [Docker](#12-docker)._

```bash
export GOOGLE_APPLICATION_CREDENTIALS=~/credentials/datacatalog-fileset-processor-sa.json
```

## 2. Load Templates from CSV file

### 2.1. Create a CSV file representing the Templates to be created

Templates are composed of as many lines as required to represent all of their fields. The columns are
described as follows:

| Column                 | Description                                    | Mandatory |
| ---                    | ---                                            | ---       |
| **template_name**      | Resource name of the Tag Template for the Tag. | Y         |
| **display_name**       | Resource name of the Tag Template for the Tag. | Y         |
| **field_id**           | Id of the Tag Template field.                  | Y         |
| **field_display_name** | Display name of the Tag Template field.        | Y         |
| **field_type**         | Type of the Tag Template field.                | Y         |
| **enum_values**        | Values for the Enum field.                     | N         |


### 4.2. Run the datacatalog-fileset-processor script - Create the Tag Templates

- Python + virtualenv

```bash
datacatalog-fileset-processor filesets create --csv-file CSV_FILE_PATH
```

### 4.3. Run the datacatalog-fileset-processor script - Delete the Tag Templates

- Python + virtualenv

```bash
datacatalog-fileset-processor filesets delete --csv-file CSV_FILE_PATH
```

*TIPS* 
- [sample-input/create-filesets][4] for reference;


[1]: https://circleci.com/gh/mesmacosta/datacatalog-fileset-processor.svg?style=svg
[2]: https://circleci.com/gh/mesmacosta/datacatalog-fileset-processor
[3]: https://virtualenv.pypa.io/en/latest/
[4]: https://github.com/mesmacosta/datacatalog-fileset-processor/tree/master/sample-input/create-filesets
[5]: https://img.shields.io/pypi/v/datacatalog-fileset-processor.svg?force_cache=true
[6]: https://pypi.org/project/datacatalog-fileset-processor/
[7]: https://img.shields.io/github/license/mesmacosta/datacatalog-fileset-processor.svg
[8]: https://img.shields.io/github/issues/mesmacosta/datacatalog-fileset-processor.svg
[9]: https://github.com/mesmacosta/datacatalog-fileset-processor/issues
