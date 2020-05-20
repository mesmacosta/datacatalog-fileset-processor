# Datacatalog Fileset Processor 

[![CircleCI][1]][2] [![PyPi][5]][6] [![License][7]][7] [![Issues][8]][9]

A package to manage Google Cloud Data Catalog Fileset scripts.

**Disclaimer: This is not an officially supported Google product.**

<!--
  ⚠️ DO NOT UPDATE THE TABLE OF CONTENTS MANUALLY ️️⚠️
  run `npx markdown-toc -i README.md`.

  Please stick to 80-character line wraps as much as you can.
-->

## Table of Contents

<!-- toc -->

- [Executing in Cloud Shell](#executing-in-cloud-shell)
- [1. Environment setup](#1-environment-setup)
  * [1.1. Python + virtualenv](#11-python--virtualenv)
    + [1.1.1. Install Python 3.6+](#111-install-python-36)
    + [1.1.2. Get the source code](#112-get-the-source-code)
    + [1.1.3. Create and activate an isolated Python environment](#113-create-and-activate-an-isolated-python-environment)
    + [1.1.4. Install the package](#114-install-the-package)
  * [1.2. Docker](#12-docker)
  * [1.3. Auth credentials](#13-auth-credentials)
    + [1.3.1. Create a service account and grant it below roles](#131-create-a-service-account-and-grant-it-below-roles)
    + [1.3.2. Download a JSON key and save it as](#132-download-a-json-key-and-save-it-as)
    + [1.3.3. Set the environment variables](#133-set-the-environment-variables)
- [2. Create Filesets from CSV file](#2-create-filesets-from-csv-file)
  * [2.1. Create a CSV file representing the Entry Groups and Entries to be created](#21-create-a-csv-file-representing-the-entry-groups-and-entries-to-be-created)
  * [2.2. Run the datacatalog-fileset-processor script - Create the Filesets Entry Groups and Entries](#22-run-the-datacatalog-fileset-processor-script---create-the-filesets-entry-groups-and-entries)
  * [2.3. Run the datacatalog-fileset-processor script - Delete the Filesets Entry Groups and Entries](#23-run-the-datacatalog-fileset-processor-script---delete-the-filesets-entry-groups-and-entries)

<!-- tocstop -->

-----

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

## 2. Create Filesets from CSV file

### 2.1. Create a CSV file representing the Entry Groups and Entries to be created

Filesets are composed of as many lines as required to represent all of their fields. The columns are
described as follows:

| Column                        | Description               | Mandatory |
| ---                           | ---                       | ---       |
| **entry_group_name**          | Entry Group Name.         | Y         |
| **entry_group_display_name**  | Entry Group Display Name. | Y         |
| **entry_group_description**   | Entry Group Description.  | Y         |
| **entry_id**                  | Entry ID.                 | Y         |
| **entry_display_name**        | Entry Display Name.       | Y         |
| **entry_description**         | Entry Description.        | Y         |
| **entry_file_patterns**       | Entry File Patterns.      | Y         |
| **schema_column_name**        | Schema column name.       | N         |
| **schema_column_type**        | Schema column type.       | N         |
| **schema_column_description** | Schema column description.| N         |
| **schema_column_mode**        | Schema column mode.       | N         |


### 2.2. Run the datacatalog-fileset-processor script - Create the Filesets Entry Groups and Entries

- Python + virtualenv

```bash
datacatalog-fileset-processor filesets create --csv-file CSV_FILE_PATH
```

### 2.3. Run the datacatalog-fileset-processor script - Delete the Filesets Entry Groups and Entries

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
