# Asaniczka

The Asaniczka module provides quick functions to get up and running with a scraper.

## Installation

To install Asaniczka, you can use pip:

`pip install asaniczka`

## Note:

- Remember to run `playwright install` on cmd/terminal after installation to install playwright browsers
- Remember to lock in the version of this package that you're using. Backwards compatibilty between major & minor versions is not guranteed

## Usage

```python
import asaniczka
import asaniczka.db_tools as dbt
import asaniczka.scrape_helper as ash

# Create project folders, creates a logger instance & start a stopwatch
project = asaniczka.ProjectSetup("MyProject")

# Save content to a temporary file
project.save_temp_file("Content")

# Format an error
formatted_error = asaniczka.format_error(error)

# Make a GET request
response = asaniczka.get_request(url)

# Make a POST request
response = asaniczka.post_request(url)

# Make an asynchronous GET request
response = await asaniczka.async_get_request(url)

# Make an asynchronous POST request
response = await asaniczka.async_post_request(url)

# Generate a random ID
random_id = asaniczka.generate_random_id()

# Save a dictionary as ndjson format
asaniczka.save_ndjson({"name": "raw dict"}, f"{project.data_folder}/my.ndjson")

# Create a new directory
my_dir = asaniczka.create_dir(os.path.join(project.data_folder, "my_data"))

# Check the ratelimit of a website
rate_limit = ash.check_ratelimit("https://amazon.com")

# Sanitize a filename
clean_filename = asaniczka.sanitize_filename("my)829filename")

# Start a stopwatch and calculate time taken
stopwatch = asaniczka.Stopwatch()
time_taken = stopwatch.lap()
```
