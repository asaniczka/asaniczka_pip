"""
Asaniczka module provides quick functions to get up and running with a scraper.

## Available functions:

1. setup_logger()
2. save_temp_file()
3. format_error()
4. get_request()
5. create_dir()

## Available Classes:
1. ProjectSetup
2. Timer

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
from playwright.sync_api import sync_playwright

# pylint: disable=logging-fstring-interpolation

# # # CLASSES # # #


class ProjectSetup:
    """A class that sets up project folders and provides access to their paths.

    Args:
        `project_name`: The name of the project.

    Attributes:
        project_name
        project_folder
        data_folder
        temp_folder
        log_folder
        log_file_path
        logger
        sb_api_url
        sb_db_url
        sb_studio_url
        sb_anon_key

    Functions:
        temp_file_path()
        generate_log_file_path()
        sanitize_filename()
        save_temp_file()
        create_new_subfolder()
        get_elapsed_time()
        check_supabase_cli_installation()
        start_supabase_instance()
        stop_supabase_instance()

    Example Usage:
        project = ProjectFolders("MyProject")
    """

    def __init__(self, project_name: str) -> None:
        if not project_name:
            raise ValueError("A project name is required.")

        cwd = os.getcwd()

        self.project_name = project_name
        self.project_folder = self.create_folder(cwd, self.project_name)
        self.data_folder = self.create_folder(self.project_folder, 'data')
        self.temp_folder = self.create_folder(self.project_folder, 'temp')
        self.log_folder = self.create_folder(self.project_folder, 'logs')
        self.db_folder = self.create_folder(self.project_folder, 'databases')
        self.start_time = time.time()
        self.logger = setup_logger(self.generate_log_file_path(dated=True))

    def create_folder(self, parent: os.PathLike, child: str) -> os.PathLike:
        """Create a folder in the given parent directory"""

        folder_path = os.path.join(parent, child)
        os.makedirs(folder_path, exist_ok=True)
        return folder_path

    def generate_temp_file_path(self,
                                name: Optional[Union[str, None]] = None,
                                extension: Optional[Union[str, None]] = 'txt') -> os.PathLike:
        """Return a temporary file name as a path.

        Args:
            `name`: The file name. Defaults to random.
            `extension`: The file extension to use. Defaults to 'txt'.

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

    def generate_log_file_path(self,
                               dated: bool = False,
                               utc: bool = False) -> os.PathLike:
        """log_file_path

        Args:
            `dated`: Whether to include the date in the file name. Defaults to False.
            `utc`: Whether to use UTC time instead of local time. Defaults to False.

        Returns:
            Union[str, Path]: The path to the log file.

        """

        if utc:
            date_today = datetime.datetime.now(pytz.utc).date()
        else:
            date_today = datetime.datetime.now().date()

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
            `name`: The filename to sanitize.

        Returns:
            str: The sanitized filename.

        """
        sanitized_name = name.replace(' ', '_')
        sanitized_name = re.sub(r'[^a-zA-Z\d_]', '', sanitized_name)
        sanitized_name = sanitized_name[:100]

        return sanitized_name

    def save_temp_file(self,
                       content: str | set | list | dict,
                       extension: Optional[Union[None, str]] = None,
                       file_name: Optional[Union[None, str]] = None) -> None:
        """Saves the given content to a temporary file in the specified temp folder.

        Only use this for quick saves. For more complex uses, use `asaniczka.save_temp_file()`

        Args:
            `content`: The content to be written to the temporary file. Lists,sets will be formatted with newlines
            `extension`: The file extension of the temporary file.
            `file_name`: The name of the temporary file.

        Returns:
            None

        """

        # format the content to a string
        if isinstance(content, list):
            string_content = '\n'.join([str(item) for item in content])
            if not extension:
                extension = 'txt'
        elif isinstance(content, set):
            string_content = '\n'.join([str(item) for item in content])
            if not extension:
                extension = 'txt'
        elif isinstance(content, dict):
            string_content = json.dumps(content)
            if not extension:
                extension = 'json'
        else:
            string_content = content
            if not extension:
                extension = 'txt'

        if not file_name:
            file_name = f"{str(datetime.datetime.now().date())}_{''.join(random.choices(string.ascii_lowercase, k=20))}"

        extension = extension.replace('.', '')

        # now save the temp file
        with open(os.path.join(self.temp_folder, f'{file_name}.{extension}'),
                  'w', encoding='utf-8') as temp_file:
            temp_file.write(string_content)

    def get_elapsed_time(self,
                         return_mins: bool = False,
                         full_decimals: bool = False) -> float:
        """
        Calculates the elapsed time since starting the project.

        Args:
            `return_mins`: Whether to return the time in minutes. Defaults to False.
            `full_decimals`: Whether to return all decimal places of the time. Defaults to False.

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
        `start_time`: The start time of the timer.

    Methods:
        `lap()`: Calculates the elapsed time since starting the timer.

    """

    def __init__(self) -> None:
        self.start_time = time.time()

    def lap(self, return_mins: bool = False, full_decimals: bool = False) -> float:
        """
        Calculates the elapsed time since starting the timer.

        Args:
            `return_mins`: Whether to return the time in minutes. Defaults to False.
            `full_decimals`: Whether to return all decimal places of the time. Defaults to False.

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
                 file_level='DEBUG',
                 disable_root_logger=True) -> logging:
    """Set up a logger and return the logger instance.

    Args:
        `log_file_path` : The path of the log file.
        `stream`: Whether to create a stream handler (default: True)
        `file`: Whether to create a file handler (default: True)
        `stream_level` : level of stream handler. Must be valid logging level
        `disable_root_logger`: Set the root logger to critical only

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

    if disable_root_logger:
        logging.getLogger().setLevel(logging.CRITICAL)

    logger = logging.getLogger('asaniczka')
    logger.setLevel(logging.DEBUG)  # set the logging level to debug

    log_format = logging.Formatter(
        '%(asctime)s :   %(levelname)s   :  %(module)s  :   %(message)s')

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
        `temp_folder`: The path to the temporary folder.
        `content`: The content to be written to the temporary file. Lists,sets will be formatted with newlines
        `file_name`: The name of the temporary file.
        `extension`: The file extension of the temporary file.

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
        `error`: The error string to be formatted.

    Returns:
        str: The formatted error string.

    Example Usage:
        `formatted_error = asaniczka.format_error(error)`
    """

    error_type = str(type(error))
    error = str(error).replace('\n', '')

    formatted_error = f'Error Type: {error_type}, Error: {error}'

    return formatted_error


def helper_get_request_no_proxy(url: str,
                                headers: dict,
                                timeout: int,
                                session: requests.Session = None) -> str | None:
    """Helper function for asaniczka module. Only for internal use"""

    if session:
        response = session.get(url, headers=headers, timeout=timeout)
    else:
        response = requests.get(url, headers=headers, timeout=timeout)

    return response


def get_request(
        url: str,
        silence_exceptions: bool = False,
        logger: Optional[Union[None, logging.Logger]] = None,
        logger_level_debug: Optional[bool] = False,
        proxy: Union[str, None] = None,
        session: requests.Session = None,
        retry_sleep_time: int = 5,
        timeout: int = 45) -> str | None:
    """
    Makes a basic HTTP GET request to the given URL.

    Args:
        `url`: The URL to make the request to.
        `silence_exceptions`: Will not raise any exceptions. Use logger_level_debug to supress errors in the console
        `logger: The logger instance to log warnings. 
        `logger_level_debug`: Whether to log warnings at debug level. 
        `proxy`: proxy to use.
        `sessions`: a requests session if you decide to use one

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
            if proxy:
                response = requests.get(
                    url, headers=headers, timeout=timeout, proxies={
                        'http': proxy,
                        'https': proxy
                    })
            else:
                response = helper_get_request_no_proxy(
                    url, headers=headers, timeout=timeout, session=session)
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
                    Status code %d, URL: %s, Response text: %s',
                    response.status_code, url, format_error(response.text))
            else:
                logger.warning(
                    'Failed to get request. \
                    Status code %d, URL: %s, Response text: %s',
                    response.status_code, url, format_error(response.text))

        if response.status_code == 420 \
                or response.status_code == 429 \
                or response.status_code >= 500:
            # sleep 1 second and incrase retries
            time.sleep(retry_sleep_time)
            retries += 1
            continue

        if not silence_exceptions:
            raise RuntimeError(f'Response code is neither 200 nor error. \
                                Last status code {response.status_code}, \
                                Response text: {format_error(response.text)}')
        break

    # raise an error if we tried more than 5 and still failed
    if retries >= 5:
        if not silence_exceptions:
            raise RuntimeError(f'No response from website. \
                                Last status code {response.status_code}, \
                                Response text: {format_error(response.text)}')

    return content


async def async_get_request(
        url: str,
        silence_exceptions: bool = False,
        logger: Optional[Union[None, logging.Logger]] = None,
        logger_level_debug: Optional[bool] = False,
        proxy: Union[str, None] = None,
        timeout: int = 45,
        retry_sleep_time: int = 5) -> str | None:
    """
    Makes an async HTTP GET request to the given URL.

    Args:
        `url`: The URL to make the request to.
        `silence_exceptions`: Will not raise any exceptions. Use logger_level_debug to suppress errors in the console.
        `logger`: The logger instance to log warnings.
        `logger_level_debug`: Whether to log warnings at debug level.
        `proxy`: Proxy to use.

    Returns:
        str: The content of the response if the request was successful.

    Raises:
        RuntimeError: If the request failed after 5 retries.

    Example Usage:
        `response_content = await async_get_request("https://example.com", logger)`
    """
    content = None
    retries = 0

    while retries < 5:
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0'
        }
        try:
            async with httpx.AsyncClient(proxies=proxy) as client:
                if proxy:
                    response = await client.get(url, headers=headers, timeout=timeout)
                else:
                    response = await client.get(url, headers=headers, timeout=timeout)
        except Exception as error:
            if logger_level_debug:
                logger.debug('Failed to get request. %s', error)
            else:
                logger.warning('Failed to get request. %s', error)

            retries += 1
            continue

        if response.status == 200:
            # Do the okay things
            content = response.text()
            break

        if logger:
            # If logger level is set to debug, log at debug level, otherwise it's a warning
            if logger_level_debug:
                logger.debug(
                    'Failed to get request. Status code %d, Response text: %s',
                    response.status, format_error(await response.text())
                )
            else:
                logger.warning(
                    'Failed to get request. Status code %d, Response text: %s',
                    response.status, format_error(await response.text())
                )

        if response.status == 420 or response.status == 429 or response.status >= 500:
            await asyncio.sleep(retry_sleep_time)
            retries += 1
            continue

        if not silence_exceptions:
            raise RuntimeError(
                f'Response code is neither 200 nor error. Last status code {response.status}, \
                 Response text: {format_error(await response.text())}'
            )
        break

    if retries >= 5:
        if not silence_exceptions:
            raise RuntimeError(
                f'No response from website. Last status code {response.status}, \
                Response text: {format_error(await response.text())}'
            )

    return content


def post_request(
        url: str,
        headers: dict = None,
        payload: str = None,
        silence_exceptions: bool = False,
        logger: Optional[Union[None, logging.Logger]] = None,
        logger_level_debug: Optional[bool] = False,
        proxy: Union[str, None] = None,
        retry_sleep_time: int = 5,
        timeout: int = 45) -> str | None:
    """
    Makes a basic HTTP POST request to the given URL.

    Args:
        `url`: The URL to make the request to.
        `headers`: The headers for the request.
        `payload`: The payload for the request.
        `silence_exceptions`: Will not raise any exceptions. Use logger_level_debug to suppress errors in the console.
        `logger`: The logger instance to log warnings.
        `logger_level_debug`: Whether to log warnings at debug level.
        `proxy`: Proxy to use.

    Returns:
        str: The content of the response if the request was successful. Returns None if the request failed.

    Raises:
        RuntimeError: If the request failed after 5 retries.

    Example Usage:
        `response_content = asaniczka.post_request("https://example.com", headers, payload, logger)`
    """
    content = None
    retries = 0
    while retries < 5:
        if not headers:
            headers = {
                'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0'
            }
        try:
            if proxy:
                response = requests.post(
                    url, headers=headers, data=payload, timeout=timeout, proxies={
                        'http': proxy,
                        'https': proxy
                    })
            else:
                response = requests.post(
                    url, headers=headers, data=payload, timeout=timeout)
        # pylint: disable=broad-except
        except Exception as error:
            if logger_level_debug:
                logger.debug(
                    'Failed to POST request. %s', format_error(error))
            else:
                logger.warning(
                    'Failed to POST request. %s', format_error(error))

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

        if response.status_code == 420 \
                or response.status_code == 429 \
                or response.status_code >= 500:
            # sleep 5 second and incrase retries
            time.sleep(retry_sleep_time)
            retries += 1
            continue

        if not silence_exceptions:
            raise RuntimeError(f'Response code is neither 200 nor error. \
                                Last status code {response.status_code}, \
                                Response text: {format_error(response.text)}')
        break

    # raise an error if we tried more than 5 and still failed
    if retries >= 5:
        if not silence_exceptions:
            raise RuntimeError(f'No response from website. \
                                Last status code {response.status_code}, \
                                Response text: {format_error(response.text)}')

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
        timeout: int = 45) -> str | None:
    """
    Makes a basic HTTP POST request to the given URL.

    Args:
        `url`: The URL to make the request to.
        `headers`: The headers for the request.
        `payload`: The payload for the request.
        `silence_exceptions`: Will not raise any exceptions. Use logger_level_debug to suppress errors in the console.
        `logger`: The logger instance to log warnings.
        `logger_level_debug`: Whether to log warnings at debug level.
        `proxy`: Proxy to use.

    Returns:
        str: The content of the response if the request was successful. Returns None if the request failed.

    Raises:
        RuntimeError: If the request failed after 5 retries.

    Example Usage:
        `response_content = asaniczka.post_request("https://example.com", headers, payload, logger)`
    """
    content = None
    retries = 0
    while retries < 5:
        if not headers:
            headers = {
                'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0'
            }
        try:
            if proxy:
                async with httpx.AsyncClient(proxies=proxy) as client:
                    response = await client.post(
                        url, headers=headers, data=payload, timeout=timeout)
            else:
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        url, headers=headers, data=payload, timeout=timeout)
        # pylint: disable=broad-except
        except Exception as error:
            if logger_level_debug:
                logger.debug(
                    'Failed to POST request. %s', format_error(error))
            else:
                logger.warning(
                    'Failed to POST request. %s', format_error(error))

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

        if response.status_code == 420 \
                or response.status_code == 429 \
                or response.status_code >= 500:
            # sleep 5 second and incrase retries
            asyncio.sleep(retry_sleep_time)
            retries += 1
            continue

        if not silence_exceptions:
            raise RuntimeError(f'Response code is neither 200 nor error. \
                                Last status code {response.status_code}, \
                                Response text: {format_error(response.text)}')
        break

    # raise an error if we tried more than 5 and still failed
    if retries >= 5:
        if not silence_exceptions:
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
