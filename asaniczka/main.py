"""
### Introduction
This module is a tool that helps in setting up projects, logging activities, making HTTP requests, and saving data in various formats.

### Highlevel Overview
- The script includes functions to:
  - Create project structures.
  - Generate temporary file and log file paths.
  - Save content to temporary files.
  - Calculate elapsed time.
  - Set up loggers for logging activities.
  - Sanitize filenames and remove special symbols.
  - Perform basic HTTP GET and POST requests.
  - Save data in JSON and ndjson formats.
  - Generate random IDs for identification.

"""

import logging
import time
from typing import Optional, Union
import os
import string
import random
import datetime
import re
import json
import asyncio

import httpx
import pytz
import requests

# pylint: disable=logging-fstring-interpolation

# # # CLASSES # # #


class ProjectSetup:
    """
    Responsible for setting up a project with necessary folders and properties.

    ### Responsibility:
    - Initialize the project with a project name and path, creating necessary folders.
    - Generate temporary file paths, log file paths, and sanitize file names.
    - Save content to a temporary file in the project's temp folder.
    - Calculate the elapsed time since starting the project.

    ### Args:
        - `project_name`: The name of the project.
        - `project_path`: The path where the project folders will be created. Defaults to the current directory.

    ### Attributes:
    - project_name: The name of the project.
    - project_folder: The project's main folder path.
    - data_folder: The folder path for project data.
    - temp_folder: The folder path for temporary files.
    - log_folder: The folder path for log files.
    - db_folder: The folder path for project databases.
    - start_time: The starting time of the project.
    - logger: Logger object for logging.

    ### Functions:
    - create_folder: Create a folder in the given parent directory.
    - generate_temp_file_path: Generate a temporary file path with optional name and extension.
    - generate_log_file_path: Generate a log file path with optional date and UTC settings.
    - sanitize_filename: Sanitize a given filename.
    - save_temp_file: Save content to a temporary file in the project's temp folder.
    - get_elapsed_time: Calculate the elapsed time since starting the project.
    """

    def __init__(self, project_name: str, project_path: os.PathLike = None) -> None:
        if not project_name:
            raise ValueError("A project name is required.")

        self.project_name = project_name

        if not project_path:
            self.project_folder = self.create_folder(os.getcwd(), self.project_name)
        else:
            self.project_folder = self.create_folder(project_path, self.project_name)

        self.data_folder = self.create_folder(self.project_folder, "data")
        self.temp_folder = self.create_folder(self.project_folder, "temp")
        self.log_folder = self.create_folder(self.project_folder, "logs")
        self.db_folder = self.create_folder(self.project_folder, "databases")
        self.start_time = time.time()
        self.logger = setup_logger(self.generate_log_file_path(dated=True))

    def create_folder(self, parent: os.PathLike, child: str) -> os.PathLike:
        """Create a folder in the given parent directory"""

        folder_path = os.path.join(parent, child)
        os.makedirs(folder_path, exist_ok=True)
        return folder_path

    def generate_temp_file_path(
        self,
        name: Optional[Union[str, None]] = None,
        extension: Optional[Union[str, None]] = "txt",
    ) -> os.PathLike:
        """
        Return a temporary file name as a path.

        Responsibility:
        - Generate a temporary file path with a given name and extension.
        - Sanitize the filename by removing any invalid characters.

        Args:
            - `name`: The file name. Defaults to random.
            - `extension`: The file extension to use. Defaults to 'txt'.

        Returns:
            - os.PathLike: The path to the created temporary file.

        """

        extension = extension.strip().replace(".", "")
        if not name:
            file_name = "".join(random.choices(string.ascii_lowercase, k=20))
        else:
            file_name = name.strip()
            file_name = self.sanitize_filename(file_name)

        temp_file_path = os.path.join(self.temp_folder, f"{file_name}.{extension}")

        return temp_file_path

    def generate_log_file_path(
        self, dated: bool = False, utc: bool = False
    ) -> os.PathLike:
        """
        Generate a log file path with optional date and UTC settings.

        Responsibility:
        - Generate a log file path with the project name.
        - Include date in the file name if specified.
        - Use UTC time if specified.

        Args:
            - `dated`: Whether to include the date in the file name. Defaults to False.
            - `utc`: Whether to use UTC time instead of local time. Defaults to False.

        Returns:
            - os.PathLike: The path to the log file.
        """

        if utc:
            date_today = datetime.datetime.now(pytz.utc).date()
        else:
            date_today = datetime.datetime.now().date()

        if dated:
            log_file_path = os.path.join(
                self.log_folder, f"{str(date_today)}_{self.project_name}.log"
            )
        else:
            log_file_path = os.path.join(self.log_folder, f"{self.project_name}.log")

        return log_file_path

    @staticmethod
    def sanitize_filename(name) -> str:
        """
        Static function for backwards compatibility
        """
        return sanitize_filename(name)

    def save_temp_file(
        self,
        content: str | set | list | dict,
        extension: Optional[Union[None, str]] = None,
        file_name: Optional[Union[None, str]] = None,
    ) -> None:
        """
        Save the given content to a temporary file in the project temp folder.

        Responsibility:
        - Save the content to a temporary file in the project temp folder.
        - Recommend using `asaniczka.save_file()` for more complex operations.

        Args:
            - `content`: The content to be written to the temporary file. Lists, sets will be formatted with newlines. For JSON lists, send json as the extension.
            - `extension`: The file extension of the temporary file.
            - `file_name`: The name of the temporary file.

        Returns:
            None
        """
        save_file(self.temp_folder, content, extionsion=extension, file_name=file_name)

    def get_elapsed_time(
        self, return_mins: bool = False, full_decimals: bool = False
    ) -> float:
        """
        Calculate the elapsed time since starting the project.

        Responsibility:
        - Calculate the time elapsed since the project started.
        - Allow the option to return the time in minutes.
        - Provide an option to control the number of decimal places in the time returned.

        Args:
            - `return_mins`: Whether to return the time in minutes. Defaults to False.
            - `full_decimals`: Whether to return all decimal places of the time. Defaults to False.

        Returns:
            float: The elapsed time in seconds if return_mins is False, or the elapsed time in minutes if return_mins is True.
        """

        end_time = time.time()
        elapsed_time = end_time - self.start_time

        if return_mins:
            elapsed_time = elapsed_time / 60

        if not full_decimals:
            elapsed_time = float(f"{elapsed_time:.2f}")

        return elapsed_time


class Stopwatch:
    """
    A simple stopwatch class to measure elapsed time.

    Attributes:
        - `start_time`: The start time of the timer.

    Methods:
        - `lap()`: Calculates the elapsed time since starting the timer.
    """

    def __init__(self) -> None:
        self.start_time = time.time()

    def lap(self, return_mins: bool = False, full_decimals: bool = False) -> float:
        """
        Calculates the elapsed time since starting the timer.

        ### Responsibility:
        - Calculate the elapsed time based on the difference between the end time and start time.
        - Convert the elapsed time to minutes if specified.

        ### Args:
        - `return_mins`: Whether to return the time in minutes. Defaults to False.
        - `full_decimals`: Whether to return all decimal places of the time. Defaults to False.

        ### Returns:
        - float: The elapsed time in seconds if return_mins is False, or the elapsed time in minutes if return_mins is True.
        """

        end_time = time.time()
        elapsed_time = end_time - self.start_time

        if return_mins:
            elapsed_time = elapsed_time / 60

        if not full_decimals:
            elapsed_time = float(f"{elapsed_time:.2f}")

        return elapsed_time


# # # FUNCTIONS # # #


def sanitize_filename(name: str, uniqify: bool = False) -> str:
    """
    Remove symbols from a filename and return a sanitized version.

    ### Responsibility:
    - Remove special symbols from the given filename to make it suitable for use as a file name.
    - Optionally add a random integer at the end of the filename if the `uniqify` parameter is set to True.

    ### Args:
    - `name`: The filename to sanitize.
    - `uniqify`: Whether to add a random integer at the end of the sanitized filename. Defaults to False.

    ### Returns:
    - str: The sanitized filename.
    """
    sanitized_name = name.replace(" ", "_")
    sanitized_name = re.sub(r"[^a-zA-Z\d_]", "", sanitized_name)
    sanitized_name = sanitized_name[:100]

    if uniqify:
        sanitized_name = (
            sanitized_name + "_" + str(random.randint(10000, 99999999999999))
        )

    return sanitized_name


def setup_logger(
    log_file_path: os.PathLike = None,
    stream=True,
    file=True,
    stream_level="INFO",
    file_level="DEBUG",
) -> logging:
    """
    Set up a logger and return the logger instance.

    ### Responsibility:
    - Configure a logger with specific handlers (stream and file) based on the provided parameters.
    - Set the log level and format for both handlers.
    - Return the configured logger instance.

    ### Args:
    - `log_file_path` : The path of the log file.
    - `stream`: Whether to create a stream handler. Defaults to True.
    - `file`: Whether to create a file handler. Defaults to True.
    - `stream_level` : Level of the stream handler. Must be a valid logging level.
    - `file_level`: Level of the file handler. Must be a valid logging level.

    ### Returns:
    - logging.Logger: The configured logger instance.

    ### Raises:
    - `ValueError`: If the provided stream_level or file_level is not a valid logging level.
    """

    level_name_to_int_lookup = {
        "notset": 0,
        "debug": 10,
        "info": 20,
        "warning": 30,
        "error": 40,
        "critical": 50,
    }

    logger = logging.getLogger("asaniczka")
    logger.setLevel(logging.DEBUG)  # set the logging level to debug

    log_format = logging.Formatter(
        "%(asctime)s :   %(levelname)s   :  %(module)s  :   %(message)s"
    )

    # init the console logger
    if stream:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(level_name_to_int_lookup[stream_level.strip().lower()])
        stream_handler.setFormatter(log_format)  # add the format
        logger.addHandler(stream_handler)

    # init the file logger
    if file:
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setFormatter(log_format)
        file_handler.setLevel(level_name_to_int_lookup[file_level.strip().lower()])
        logger.addHandler(file_handler)

    return logger


def save_file(
    folder: os.PathLike,
    content: str | set | list | dict,
    extionsion: Optional[Union[str, None]] = None,
    file_name: Optional[Union[str, None]] = None,
) -> None:
    """
    Saves the given content to a temporary file in the specified temp folder.

    ### Responsibility:
    - Formats the content based on its type (str, set, list, dict) and saves it to a temporary file in the specified folder with the given file name and extension.

    ### Args:
        - `folder`: The path to the temporary folder.
        - `content`: The content to be written to the temporary file. Lists and sets will be formatted with newlines. For JSON lists, specify the extension as "json".
        - `file_name`: The name of the temporary file.
        - `extension`: The file extension of the temporary file.

    ### Returns:
        None

    ### Example Usage:
        `save_file("/path/to/temp/folder", "This is the file content", "txt", "example_file")`
    """

    # format the content to a string
    if isinstance(content, (list, set)):
        if extionsion != "json" or extionsion != ".json":
            string_content = "\n".join([str(item) for item in content])
            if not extionsion:
                extionsion = "txt"
        else:
            string_content = json.dumps(content)
    elif isinstance(content, dict):
        string_content = json.dumps(content)
        if not extionsion:
            extionsion = "json"
    else:
        string_content = content
        if not extionsion:
            extionsion = "txt"

    if not file_name:
        file_name = f"{str(datetime.datetime.now())}_{''.join(random.choices(string.ascii_lowercase, k=20))}"

    # now save the temp file
    with open(
        os.path.join(folder, f"{file_name}.{extionsion}"), "w", encoding="utf-8"
    ) as temp_file:
        temp_file.write(string_content)


def format_error(error: str) -> str:
    """
    Removes newlines from the given error string.

    ### Responsibility:
    - Format the error string by removing newlines.

    ### Args:
    - `error`: The error string to be formatted.

    ### Returns:
    - `str`: The formatted error string.

    Example Usage:
        `formatted_error = asaniczka.format_error(error)`
    """

    error_type = str(type(error))
    error = str(error).replace("\n", "")

    formatted_error = f"Error Type: {error_type}, Error: {error}"

    return formatted_error


def helper_get_request_no_proxy(
    url: str, headers: dict, timeout: int, session: requests.Session = None
) -> str | None:
    """
    Helper function for asaniczka module. Only for internal use.

    ### Responsibility:
    - Make a GET request to a URL without using a proxy.

    ### Args:
    - `url`: The URL to make the GET request.
    - `headers`: The headers to be included in the request.
    - `timeout`: The timeout value for the request.
    - `session` (optional): A requests Session object for making the request.

    ### Returns:
    - `str` or `None`: The response from the GET request or None if an error occurred.
    """

    if session:
        response = session.get(url, headers=headers, timeout=timeout)
    else:
        response = requests.get(url, headers=headers, timeout=timeout)

    return response


def get_request(
    url: str,
    headers: dict = None,
    silence_exceptions: bool = False,
    logger: Optional[Union[None, logging.Logger]] = None,
    logger_level_debug: Optional[bool] = False,
    proxy: Union[str, None] = None,
    session: requests.Session = None,
    retry_sleep_time: int = 5,
    timeout: int = 45,
) -> str | None:
    """
    Makes a basic HTTP GET request to the given URL.

    ### Responsibility:
    - Make a GET request to a URL with options for handling exceptions, logging, proxies, and session usage.
    - Retry the request multiple times and raise an error if unsuccessful after 5 retries.

    ### Args:
    - `url`: The URL to make the GET request.
    - `headers`: (optional) The headers to be included in the request.
    - `silence_exceptions`: Will not raise any exceptions if set to True.
    - `logger`: The logger instance to log warnings.
    - `logger_level_debug`: Whether to log warnings at debug level.
    - `proxy`: Proxy to use for the request.
    - `session`: A requests Session object to use for the request.
    - `retry_sleep_time`: The time to sleep between retries.
    - `timeout`: The timeout value for the request.

    ### Returns:
    - `str` or `None`: The content of the response if the request was successful, or None if an error occurred.

    ### Raises:
    - `RuntimeError`: If the request failed after 5 retries.
    """
    content = None
    retries = 0
    while retries < 5:
        if not headers:
            headers = {
                "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0"
            }
        try:
            if proxy:
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=timeout,
                    proxies={"http": proxy, "https": proxy},
                )
            else:
                response = helper_get_request_no_proxy(
                    url, headers=headers, timeout=timeout, session=session
                )
        # pylint: disable=broad-except
        except Exception as error:
            if logger:
                if logger_level_debug:
                    logger.debug("Failed to GET request. %s", format_error(error))
                else:
                    logger.warning("Failed to GET request. %s", format_error(error))
            else:
                print("Failed to GET request. %s", format_error(error))

            retries += 1
            continue

        if response.status_code == 200:
            # do the okay things
            content = response.text
            break

        # if not okay, then start logging and retrying
        if logger:
            # if logger level is said to be debug, do debug, otherwise it's a warning
            if logger_level_debug:
                logger.debug(
                    "Failed to get request. \
                    Status code %d, URL: %s, Response text: %s",
                    response.status_code,
                    url,
                    format_error(response.text),
                )
            else:
                logger.warning(
                    "Failed to get request. \
                    Status code %d, URL: %s, Response text: %s",
                    response.status_code,
                    url,
                    format_error(response.text),
                )

        if (
            response.status_code == 420
            or response.status_code == 429
            or response.status_code >= 500
        ):
            # sleep 1 second and incrase retries
            time.sleep(retry_sleep_time)
            retries += 1
            continue

        if not silence_exceptions:
            raise RuntimeError(
                f"Response code is neither 200 nor error. \
                                Last status code {response.status_code}, \
                                Response text: {format_error(response.text)}"
            )
        break

    # raise an error if we tried more than 5 and still failed
    if retries >= 5:
        if not silence_exceptions:
            raise RuntimeError(
                f"No response from website. \
                                Last status code {response.status_code}, \
                                Response text: {format_error(response.text)}"
            )

    return content


async def async_get_request(
    url: str,
    headers: dict = None,
    silence_exceptions: bool = False,
    logger: Optional[Union[None, logging.Logger]] = None,
    logger_level_debug: Optional[bool] = False,
    proxy: Union[str, None] = None,
    timeout: int = 45,
    retry_sleep_time: int = 5,
) -> str | None:
    """
    Makes an async HTTP GET request to the given URL.

    ### Responsibility:
    - Make an asynchronous GET request to a URL with options for handling exceptions, logging, proxies.
    - Retry the request multiple times and raise an error if unsuccessful after 5 retries.

    ### Args:
    - `url`: The URL to make the GET request.
    - `headers`: (optional) The headers to be included in the request.
    - `silence_exceptions`: Will not raise any exceptions if set to True.
    - `logger`: The logger instance to log warnings.
    - `logger_level_debug`: Whether to log warnings at debug level.
    - `proxy`: Proxy to use for the request.
    - `timeout`: The timeout value for the request.
    - `retry_sleep_time`: The time to sleep between retries.

    ### Returns:
    - `str` or `None`: The content of the response if the request was successful, or None if an error occurred.

    ### Raises:
    - `RuntimeError`: If the request failed after 5 retries.

    Example Usage:
        `response_content = await async_get_request("https://example.com", headers={"User-Agent": "Mozilla/5.0"}, logger=logger)`
    """
    retries = 0

    while retries < 5:
        if not headers:
            headers = {
                "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0"
            }
        try:
            async with httpx.AsyncClient(proxies=proxy) as client:
                if proxy:
                    response = await client.get(url, headers=headers, timeout=timeout)
                else:
                    response = await client.get(url, headers=headers, timeout=timeout)
        except Exception as error:
            if logger:
                if logger_level_debug:
                    logger.debug("Failed to GET request. %s", format_error(error))
                else:
                    logger.warning("Failed to GET request. %s", format_error(error))
            else:
                print("Failed to GET request. %s", format_error(error))

            retries += 1
            continue

        if response.status_code == 200:
            # Do the okay things
            content = response.text
            return content

        if logger:
            # If logger level is set to debug, log at debug level, otherwise it's a warning
            if logger_level_debug:
                logger.debug(
                    "Failed to get request. Status code %d, Response text: %s",
                    response.status_code,
                    format_error(await response.text()),
                )
            else:
                logger.warning(
                    "Failed to get request. Status code %d, Response text: %s",
                    response.status_code,
                    format_error(await response.text()),
                )

        if (
            response.status_code == 420
            or response.status_code == 429
            or response.status_code >= 500
        ):
            await asyncio.sleep(retry_sleep_time)
            retries += 1
            continue

        if not silence_exceptions:
            raise RuntimeError(
                f"Response code is neither 200 nor error. Last status code {response.status_code}, \
                 Response text: {format_error(await response.text())}"
            )
        break

    if retries >= 5:
        if not silence_exceptions:
            raise RuntimeError(
                f"No response from website. Last status code {response.status_code}, \
                Response text: {format_error(await response.text())}"
            )

    return None


def post_request(
    url: str,
    headers: dict = None,
    payload: str = None,
    silence_exceptions: bool = False,
    logger: Optional[Union[None, logging.Logger]] = None,
    logger_level_debug: Optional[bool] = False,
    proxy: Union[str, None] = None,
    retry_count: int = 5,
    retry_sleep_time: int = 5,
    timeout: int = 45,
) -> str | None:
    """
    Makes a basic HTTP GET request to the given URL.

    ### Responsibility:
    - Make a GET request to a URL with options for handling exceptions, logging, proxies, payload, and session usage.
    - Retry the request multiple times and raise an error if unsuccessful after specified retries.

    ### Args:
    - `url`: The URL to make the GET request.
    - `headers`: The headers to be included in the request.
    - `payload`: The data to be sent in the request body.
    - `silence_exceptions`: Will not raise any exceptions if set to True.
    - `logger`: The logger instance to log warnings.
    - `logger_level_debug`: Whether to log warnings at debug level.
    - `proxy`: Proxy to use for the request.
    - `retry_count`: Number of times to retry the request.
    - `retry_sleep_time`: The time to sleep between retries.
    - `timeout`: The timeout value for the request.

    ### Returns:
    - `str` or `None`: The content of the response if the request was successful, or None if an error occurred.

    ### Raises:
    - `RuntimeError`: If the request failed after the specified number of retries.

    Example Usage:
        `response_content = asaniczka.post_request("https://example.com", headers, payload, logger)`
    """
    content = None
    retries = 0
    while retries <= retry_count:
        if not headers:
            headers = {
                "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0"
            }
        try:
            if proxy:
                response = requests.post(
                    url,
                    headers=headers,
                    data=payload,
                    timeout=timeout,
                    proxies={"http": proxy, "https": proxy},
                )
            else:
                response = requests.post(
                    url, headers=headers, data=payload, timeout=timeout
                )
        # pylint: disable=broad-except
        except Exception as error:
            if logger:
                if logger_level_debug:
                    logger.debug("Failed to POST request. %s", format_error(error))
                else:
                    logger.warning("Failed to POST request. %s", format_error(error))
            else:
                print("Failed to POST request. %s", format_error(error))

            retries += 1
            continue

        if response.status_code == 200:
            # do the okay things
            content = response.text
            break

        # if not okay, then start logging and retrying
        if logger:
            # if logger level is said to be debug, do debug, otherwise it's a warning
            if logger_level_debug:
                logger.debug(
                    "Failed to POST request. \
                    Status code %d, Response text: %s",
                    response.status_code,
                    format_error(response.text),
                )
            else:
                logger.warning(
                    "Failed to POST request. \
                    Status code %d, Response text: %s",
                    response.status_code,
                    format_error(response.text),
                )

        if (
            response.status_code == 420
            or response.status_code == 429
            or response.status_code >= 500
        ):
            # sleep 5 second and incrase retries
            time.sleep(retry_sleep_time)
            retries += 1
            continue

        if not silence_exceptions:
            raise RuntimeError(
                f"Response code is neither 200 nor error. \
                                Last status code {response.status_code}, \
                                Response text: {format_error(response.text)}"
            )
        break

    # raise an error if we tried more than 5 and still failed
    if retries > retry_count:
        if not silence_exceptions:
            raise RuntimeError(
                f"No response from website. \
                                Last status code {response.status_code}, \
                                Response text: {format_error(response.text)}"
            )

    return content


async def async_post_request(
    url: str,
    headers: dict = None,
    payload: str = None,
    silence_exceptions: bool = False,
    logger: Optional[Union[None, logging.Logger]] = None,
    logger_level_debug: Optional[bool] = False,
    proxy: Union[str, None] = None,
    retry_sleep_time: int = 5,
    timeout: int = 45,
) -> str | None:
    """
    Makes an asynchronous HTTP POST request to the given URL.

    ### Responsibility:
    - Make an asynchronous POST request to a URL with options for handling exceptions, logging, proxies, payload, and session usage.
    - Retry the request multiple times and raise an error if unsuccessful after 5 retries.

    ### Args:
    - `url`: The URL to make the POST request.
    - `headers`: The headers to be included in the request.
    - `payload`: The data to be sent in the request body.
    - `silence_exceptions`: Will not raise any exceptions if set to True.
    - `logger`: The logger instance to log warnings.
    - `logger_level_debug`: Whether to log warnings at debug level.
    - `proxy`: Proxy to use for the request.
    - `retry_sleep_time`: The time to sleep between retries.
    - `timeout`: The timeout value for the request.

    ### Returns:
    - `str` or `None`: The content of the response if the request was successful, or None if an error occurred.

    ### Raises:
    - `RuntimeError`: If the request failed after 5 retries.

    Example Usage:
        `response_content = asaniczka.async_post_request("https://example.com", headers, payload, logger)`
    """
    content = None
    retries = 0
    while retries < 5:
        if not headers:
            headers = {
                "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0"
            }
        try:
            if proxy:
                async with httpx.AsyncClient(proxies=proxy) as client:
                    response = await client.post(
                        url, headers=headers, data=payload, timeout=timeout
                    )
            else:
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        url, headers=headers, data=payload, timeout=timeout
                    )
        # pylint: disable=broad-except
        except Exception as error:
            if logger:
                if logger_level_debug:
                    logger.debug("Failed to POST request. %s", format_error(error))
                else:
                    logger.warning("Failed to POST request. %s", format_error(error))
            else:
                print("Failed to POST request. %s", format_error(error))

            retries += 1
            continue

        if response.status_code == 200:
            # do the okay things
            content = response.text
            break

        # if not okay, then start logging and retrying
        if logger:
            # if logger level is said to be debug, do debug, otherwise it's a warning
            if logger_level_debug:
                logger.debug(
                    "Failed to POST request. \
                    Status code %d, Response text: %s",
                    response.status_code,
                    format_error(response.text),
                )
            else:
                logger.warning(
                    "Failed to POST request. \
                    Status code %d, Response text: %s",
                    response.status_code,
                    format_error(response.text),
                )

        if (
            response.status_code == 420
            or response.status_code == 429
            or response.status_code >= 500
        ):
            # sleep 5 second and incrase retries
            asyncio.sleep(retry_sleep_time)
            retries += 1
            continue

        if not silence_exceptions:
            raise RuntimeError(
                f"Response code is neither 200 nor error. \
                                Last status code {response.status_code}, \
                                Response text: {format_error(response.text)}"
            )
        break

    # raise an error if we tried more than 5 and still failed
    if retries >= 5:
        if not silence_exceptions:
            raise RuntimeError(
                f"No response from website. \
                                Last status code {response.status_code}, \
                                Response text: {format_error(response.text)}"
            )

    return content


def save_ndjson(data: dict, file_path: str) -> None:
    """
    Saves the given data to an ndjson file.

    ### Responsibility:
    - Append the given data as a JSON line to the specified ndjson file.

    ### Args:
    - `data`: The dictionary data to be saved in ndjson format.
    - `file_path`: The path to the ndjson file to which data will be appended.

    ### Returns:
    - `None`: This function does not return anything.

    ### Raises:
    - No specific exceptions are raised by this function.

    Example Usage:
        `save_ndjson({"key": "value"}, "data.ndjson")`
    """

    with open(file_path, "a", encoding="utf-8") as dump_file:
        dump_file.write(f"{json.dumps(data)}\n")


def create_dir(folder: os.PathLike) -> os.PathLike:
    """
    Creates a directory at the given path.

    ### Responsibility:
    - Create a directory at the specified path.
    - If the directory already exists, do not raise an error.

    ### Args:
    - `folder`: The path where the directory should be created.

    ### Returns:
    - `os.PathLike`: The path of the directory that was created.

    ### Raises:
    - No specific exceptions are raised by this function.

    Example Usage:
        `new_folder = create_dir("/path/to/folder")`
    """

    os.makedirs(folder, exist_ok=True)
    return folder


def generate_random_id() -> int:
    """
    Generates a random integer ID.

    ### Responsibility:
    - Generate a random integer ID within the range [10000, 100000000000000) for unique identification purposes.

    ### Args:
    - This function does not take any arguments.

    ### Returns:
    - `int`: A randomly generated integer ID.

    ### Raises:
    - No specific exceptions are raised by this function.

    Example Usage:
        `unique_id = generate_random_id()`
    """

    return random.choice(range(10000, 100000000000000))
