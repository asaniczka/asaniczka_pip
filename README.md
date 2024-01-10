# Asaniczka

The Asaniczka module provides quick functions to get up and running with a scraper.

## Available Functions:

1. `setup_logger()`: Set up a logger and return the logger instance.
2. `save_temp_file()`: Saves the given content to a temporary file in the specified temp folder.
3. `format_error()`: Removes newlines from the given error string.
4. `get_request()`: Makes a basic HTTP GET request to the given URL.
5. `create_dir()`: Creates a new directory.
6. `steal_cookies()`: Steals the cookies from a given domain for SSRF

## Available Classes:

1. `ProjectSetup`: A class that sets up project folders and provides access to their paths.

## Installation

To install Asaniczka, you can use pip:

`pip install asaniczka`

## Note:

Remember to run `playwright install` on cmd/terminal after installation to install playwright browsers

## Usage

```python
import asaniczka
import asaniczka.db_tools as dbt
import asaniczka.scrape_helper as ash

# Create project folders
project = asaniczka.ProjectSetup("MyProject")

# Set up a logger
logger = asaniczka.setup_logger(project.log_file_path)

# Save content to a temporary file
asaniczka.save_temp_file(content, extension='txt')

# Format an error
formatted_error = asaniczka.format_error(error)

# Make a GET request
response = asaniczka.get_request(url)

# Create a new directory
my_dir = asaniczka.create_dir(os.path.join(project.data_folder,'my_data'))

# check the ratelimit of a website
rate_limit = ash.check_ratelimit('https://amazon.com')

# load a postgres db
project.start_supabase_instance()

# get all the names of tables in the db
table_names = dbt.get_sb_table_names(project, make_list=True)

# backup supabase postgres db
dbt.backup_sb_db(project)

# shutdown database
project.stop_supabase_instance()

```
