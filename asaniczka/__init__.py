"""
Asaniczka module provides quick functions to get up and running with a scraper.

## Available functions:

1. setup_logger()
2. save_temp_file()
3. format_error()
4. get_request()
5. create_dir()
6. steal_cookies()

## Available Classes:

1. ProjectSetup

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

import pytz
import requests
from playwright.sync_api import sync_playwright

# # # CLASSES # # #


class ProjectSetup:
    """A class that sets up project folders and provides access to their paths.

    This class sets up the project folder, data folder, temp folder, log folder, and log file path for a given project.
    The paths to these folders can be accessed as instance attributes.

    Args:
        `project_name (str)`: The name of the project.

    Attributes:
        :params project_name: The name of the project.
        :params project_folder: The path to the project folder.
        :params data_folder: The path to the data folder.
        :params temp_folder: The path to the temp folder.
        :params log_folder: The path to the log folder.
        :params log_file_path: The path to the log file.

    Functions:
        temp_file_path(): Return a temporary file name as a path.
        log_file_path(): log_file_path
        sanitize_filename():

    Example Usage:
        project = ProjectFolders("MyProject")
    """

    def __init__(self, project_name: str) -> None:
        if not project_name:
            raise ValueError("A project name is required.")

        self.project_name = project_name

        # create the project folder
        cwd = os.getcwd()
        self.project_folder = os.path.join(cwd, self.project_name)
        os.makedirs(self.project_folder, exist_ok=True)

        # create the data folder
        self.data_folder = os.path.join(self.project_folder, 'data')
        os.makedirs(self.data_folder, exist_ok=True)

        # make the temp folder
        self.temp_folder = os.path.join(self.project_folder, 'temp')
        os.makedirs(self.temp_folder, exist_ok=True)

        # make the log folder and log file path
        self.log_folder = os.path.join(self.project_folder, 'logs')
        os.makedirs(self.log_folder, exist_ok=True)

        self.start_time = time.time()

    def temp_file_path(self, name: Optional[Union[str, None]] = None, extension: str = 'txt') -> os.PathLike:
        """Return a temporary file name as a path.

        Args:
            `name (str, optional)`: The file name. Defaults to random.
            `extension (str, optional)`: The file extension to use. Defaults to 'txt'.

        Returns:
            Union[str, Path]: The path to the created temporary file.

        """

        extension = extension.strip().replace('.', '')
        if not name:
            file_name = ''.join(random.choices(string.ascii_lowercase, k=20))
        else:
            file_name = name.strip()
            file_name = self.sanitize_filename(file_name)

        temp_file_path = os.path.join(
            self.temp_folder, f'{file_name}.{extension}')

        return temp_file_path

    def log_file_path(self, dated: bool = False, utc: bool = False) -> os.PathLike:
        """log_file_path

        Args:
            `dated (bool, optional)`: Whether to include the date in the file name. Defaults to False.
            `utc (bool, optional)`: Whether to use UTC time instead of local time. Defaults to False.

        Returns:
            Union[str, Path]: The path to the log file.

        """

        if utc:
            date_today = datetime.datetime.now(pytz.utc).date
        else:
            date_today = datetime.datetime.now().date

        if dated:
            log_file_path = os.path.join(
                self.log_folder, f'{str(date_today)}_{self.project_name}.log')
        else:
            log_file_path = os.path.join(
                self.log_folder, f'{self.project_name}.log')

        return log_file_path

    def sanitize_filename(self, name) -> str:
        """Remove symbols from a filename and return a sanitized version.

        Args:
            `name (str)`: The filename to sanitize.

        Returns:
            str: The sanitized filename.

        """
        sanitized_name = name.replace(' ', '_')
        sanitized_name = re.sub(r'[^a-zA-Z\d_]', '', sanitized_name)
        sanitized_name = sanitized_name[:100]

        return sanitized_name

    def save_temp_file(self,
                       content: str | set | list | dict,
                       extionsion: Optional[Union[None, str]] = None,
                       file_name: Optional[Union[None, str]] = None) -> None:
        """Saves the given content to a temporary file in the specified temp folder.

        Only use this for quick saves. For more complex uses, use `asaniczka.save_temp_file()`

        Args:
            `content (str | set | list | dict)`: The content to be written to the temporary file. 
                Lists,sets will be formatted with newlines
            `extension (str)`: The file extension of the temporary file.
            `file_name (str)`: The name of the temporary file.

        Returns:
            None

        """

        # format the content to a string
        if isinstance(content, list):
            string_content = '\n'.join([str(item) for item in content])
            if not extionsion:
                extionsion = 'txt'
        elif isinstance(content, set):
            string_content = '\n'.join([str(item) for item in content])
            if not extionsion:
                extionsion = 'txt'
        elif isinstance(content, dict):
            string_content = json.dumps(content)
            if not extionsion:
                extionsion = 'json'
        else:
            string_content = content
            if not extionsion:
                extionsion = 'txt'

        if not file_name:
            file_name = f"{str(datetime.datetime.now().date)}_{''.join(random.choices(string.ascii_lowercase, k=20))}"

        # now save the temp file
        with open(os.path.join(self.temp_folder, f'{file_name}.{extionsion}'),
                  'w', encoding='utf-8') as temp_file:
            temp_file.write(string_content)

    def create_new_subfolder(self, name: str, is_temp: False) -> os.PathLike:
        """Create a new folder with the given name.

        Args:
            `name (str)`: The name of the new folder.
            `is_temp (bool, optional)`: Whether the folder should be created in the temporary folder. Defaults to False.

        Returns:
            Union[str, Path]: The path to the created folder.

        """

        if is_temp:
            folder_path = os.path.join(self.temp_folder, name)
            os.makedirs(folder_path, exist_ok=True)
        else:
            folder_path = os.path.join(self.data_folder, name)
            os.makedirs(folder_path, exist_ok=True)

        return folder_path

    def calc_elapsed_time(self, return_mins: bool = False, full_decimals: bool = False) -> float:
        """
        Calculates the elapsed time since starting the project.

        Args:
            return_mins (bool, optional): Whether to return the time in minutes. Defaults to False.
            full_decimals (bool, optional): Whether to return all decimal places of the time. Defaults to False.

        Returns:
            float: The elapsed time in seconds if return_mins is False, or the elapsed time in minutes if return_mins is True.
        """

        end_time = time.time()
        elapsed_time = end_time - self.start_time

        if return_mins:
            elapsed_time = elapsed_time/60

        if not full_decimals:
            elapsed_time = float(f"{elapsed_time:.2f}")

        return elapsed_time


class Timer:
    """
    A simple timer class to measure elapsed time.

    Attributes:
        start_time (float): The start time of the timer.

    Methods:
        lap(): Calculates the elapsed time since starting the timer.

    """

    def __init__(self) -> None:
        self.start_time = time.time()

    def lap(self, return_mins: bool = False, full_decimals: bool = False) -> float:
        """
        Calculates the elapsed time since starting the timer.

        Args:
            return_mins (bool, optional): Whether to return the time in minutes. Defaults to False.
            full_decimals (bool, optional): Whether to return all decimal places of the time. Defaults to False.

        Returns:
            float: The elapsed time in seconds if return_mins is False, or the elapsed time in minutes if return_mins is True.
        """

        end_time = time.time()
        elapsed_time = end_time - self.start_time

        if return_mins:
            elapsed_time = elapsed_time/60

        if not full_decimals:
            elapsed_time = float(f"{elapsed_time:.2f}")

        return elapsed_time

# # # FUNCTIONS # # #


def setup_logger(log_file_path: str,
                 stream=True,
                 file=True,
                 stream_level='INFO',
                 file_level='DEBUG') -> logging:
    """Set up a logger and return the logger instance.

    Args:
        log_file_path (str): The path of the log file.
        stream (bool): Whether to create a stream handler (default: True)
        file (bool): Whether to create a file handler (default: True)
        stream_level (str): level of stream handler. Must be valid logging level

    Returns:
        logging.Logger: The configured logger instance.

    Example Usage:
        `LOGGER = asaniczka.setup_logger("/path/to/log/file.log")`
    """

    level_name_to_int_lookup = {
        "notset": 0,
        "debug": 10,
        "info": 20,
        "warning": 30,
        "error": 40,
        "critical": 50
    }

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)  # set the logging level to debug

    log_format = logging.Formatter(
        '%(asctime)s :   %(levelname)s   :   %(message)s')

    # init the console logger
    if stream:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(
            level_name_to_int_lookup[stream_level.strip().lower()])
        stream_handler.setFormatter(log_format)  # add the format
        logger.addHandler(stream_handler)

    # init the file logger
    if file:
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setFormatter(log_format)
        file_handler.setLevel(
            level_name_to_int_lookup[file_level.strip().lower()])
        logger.addHandler(file_handler)

    return logger


def save_file(
        folder: os.PathLike,
        content: str | set | list | dict,
        extionsion: Optional[Union[str, None]] = None,
        file_name: Optional[Union[str, None]] = None) -> None:
    """Saves the given content to a temporary file in the specified temp folder.

    Args:
        `temp_folder (str)`: The path to the temporary folder.
        `content (str | set | list | dict)`: The content to be written to the temporary file. 
            Lists,sets will be formatted with newlines
        `file_name (str)`: The name of the temporary file.
        `extension (str)`: The file extension of the temporary file.

    Returns:
        None

    Example Usage:
        `asaniczka.save_temp_file("/path/to/temp/folder", "example_file", "This is the file content", "txt")`
    """

    # format the content to a string
    if isinstance(content, list):
        string_content = '\n'.join([str(item) for item in content])
        if not extionsion:
            extionsion = 'txt'
    elif isinstance(content, set):
        string_content = '\n'.join([str(item) for item in content])
        if not extionsion:
            extionsion = 'txt'
    elif isinstance(content, dict):
        string_content = json.dumps(content)
        if not extionsion:
            extionsion = 'json'
    else:
        string_content = content
        if not extionsion:
            extionsion = 'txt'

    if not file_name:
        file_name = f"{str(datetime.datetime.now().date)}_{''.join(random.choices(string.ascii_lowercase, k=20))}"

    # now save the temp file
    with open(os.path.join(folder, f'{file_name}.{extionsion}'),
              'w', encoding='utf-8') as temp_file:
        temp_file.write(string_content)


def format_error(error: str) -> str:
    """
    Removes newlines from the given error string.

    Args:
        `error (str)`: The error string to be formatted.

    Returns:
        str: The formatted error string.

    Example Usage:
        `formatted_error = asaniczka.format_error(error)`
    """

    error_type = str(type(error))
    error = str(error).replace('\n', '')

    formatted_error = f'Error Type: {error_type}, Error: {error}'

    return formatted_error


def get_request(
        url: str,
        silence_errors: bool = False,
        logger: Optional[Union[None, logging.Logger]] = None,
        logger_level_debug: Optional[bool] = False,) -> str | None:
    """
    Makes a basic HTTP GET request to the given URL.

    Args:
        `url (str)`: The URL to make the request to.
        `logger (Optional:None, logging.Logger)`: The logger instance to log warnings. 
                (default: None)
        `logger_level_debug (Optional:bool)`: Whether to log warnings at debug level. 
                (default: False)

    Returns:
        str: The content of the response if the request was successful.

    Raises:
        RuntimeError: If the request failed after 5 retries.

    Example Usage:
        `response_content = asaniczka.get_request("https://example.com", logger)`
    """
    content = None
    retries = 0
    while retries < 5:
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0'
        }
        try:
            response = requests.get(url, headers=headers, timeout=10)
        # pylint: disable=broad-except
        except Exception as error:
            if logger_level_debug:
                logger.debug(
                    'Failed to get request. %s', format_error(error))
            else:
                logger.warning(
                    'Failed to get request. %s', format_error(error))

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
                    'Failed to get request. \
                    Status code %d, Response text: %s',
                    response.status_code, format_error(response.text))
            else:
                logger.warning(
                    'Failed to get request. \
                    Status code %d, Response text: %s',
                    response.status_code, format_error(response.text))

        if response.status_code == 420 or response.status_code == 429 or response.status_code >= 500:
            # sleep 1 second and incrase retries
            time.sleep(5)
            retries += 1
            continue

        if not silence_errors:
            raise RuntimeError(f'Response code is neither 200 nor error. \
                                Last status code {response.status_code}, \
                                Response text: {format_error(response.text)}')

    # raise an error if we tried more than 5 and still failed
    if retries >= 5:
        if not silence_errors:
            raise RuntimeError(f'No response from website. \
                                Last status code {response.status_code}, \
                                Response text: {format_error(response.text)}')

    return content


def save_ndjson(data: dict, file_path: str) -> None:
    """Saves the given data to the same ndjson file"""
    # pylint: disable=import-outside-toplevel

    with open(file_path, 'a', encoding='utf-8') as dump_file:
        dump_file.write(f'{json.dumps(data)}\n')


def create_dir(folder: os.PathLike) -> os.PathLike:
    """creates a dir. Must send a valid path"""

    os.makedirs(folder, exist_ok=True)
    return folder


def steal_cookies(url: str) -> dict:
    """
    Gets cookies from a given domain.

    Args:
        `url (str)`: The URL from which to steal cookies.

    Returns:
        `dict`: A dictionary containing the stolen cookies, where the keys are the cookie names and the values are the cookie values.

    Raises:
        RuntimeError: If an error occurs when stealing the cookies.

    Example Usage:
        `cookies = asaniczka.steal_cookies("https://example.com")`
    """

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            page = browser.new_page()
            page.goto(url)

            cookies = page.context.cookies()

        stolen_cookie_dict = {}
        if cookies:
            for cookie in cookies:
                stolen_cookie_dict[cookie['name']] = cookie['value']

        return stolen_cookie_dict

    except Exception as error:
        raise RuntimeError(
            f'Error when stealing cookies. Please inform developer (asaniczka@gmail.com) of this error as this error is not handled. \n{format_error(error)}') from error
