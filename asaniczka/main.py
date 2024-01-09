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
import subprocess
import threading

import pytz
import requests
from playwright.sync_api import sync_playwright
import db_tools as dbt

# pylint: disable=logging-fstring-interpolation

# # # CLASSES # # #


class ProjectSetup:
    """A class that sets up project folders and provides access to their paths.

    This class sets up the project folder, data folder, temp folder, log folder, and log file path for a given project.
    The paths to these folders can be accessed as instance attributes.

    Args:
        `project_name (str)`: The name of the project.

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
        self.sb_api_url = None
        self.sb_db_url = None
        self.sb_studio_url = None
        self.sb_anon_key = None
        self.db_backup_loop = False

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

    def generate_log_file_path(self,
                               dated: bool = False,
                               utc: bool = False) -> os.PathLike:
        """log_file_path

        Args:
            `dated (bool, optional)`: Whether to include the date in the file name. Defaults to False.
            `utc (bool, optional)`: Whether to use UTC time instead of local time. Defaults to False.

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

    def get_elapsed_time(self,
                         return_mins: bool = False,
                         full_decimals: bool = False) -> float:
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

    def check_supabase_cli_installation(self) -> None:
        """function checks if supabase cli is installed on the system"""

        # check if supabase  cli is installed
        # pylint: disable=bare-except
        try:
            _ = subprocess.run('supabase', shell=True,
                               check=True, capture_output=True)
            is_supabase_installed = True
        except:
            is_supabase_installed = False

        if not is_supabase_installed:
            self.logger.critical(
                "Asaniczka can't launch Supabase. You need to install supabase first. \nhttps://supabase.com/docs/guides/cli/getting-started")
            raise RuntimeError(
                "Asaniczka can't launch Supabase. You need to install supabase first. \nhttps://supabase.com/docs/guides/cli/getting-started")

    def initialize_supabase(self, config_file_path: os.PathLike) -> None:
        """Initalizes supabase for the current project"""
        self.logger.info("Creating supabase config")
        # initialize the project setup
        process = subprocess.Popen(['supabase', 'init'], stdout=subprocess.PIPE,
                                   stdin=subprocess.PIPE, stderr=subprocess.PIPE, cwd=self.db_folder)

        _ = process.communicate(input=b'n\n')

        # replace standard supabase ports with random ports to avoid clashes with other db instances
        with open(config_file_path, 'r+', encoding='utf-8') as config_file:
            lines = config_file.readlines()
            lines = [line.strip() for line in lines]
            lines = [line.replace(
                'project_id = "databases"', f'project_id = "{self.project_name}"') for line in lines]
            ports_to_replace = [54320,  54321, 54322,
                                54323, 54324, 54325, 54326,  54327, 54328,
                                54329, 54330]

            new_port_start = random.randint(20000, 50000)
            self.logger.debug(
                f"supabase port start value is: {new_port_start}")
            new_ports_list = []
            for idx, port in enumerate(ports_to_replace):
                new_ports_list.append(new_port_start+idx)

            modified_lines = []
            for line in lines:
                for idx, port in enumerate(ports_to_replace):
                    line = line.replace(
                        str(port), str(new_ports_list[idx]))
                modified_lines.append(line)

            config_file.seek(0)
            config_file.write('\n'.join(modified_lines))
            config_file.truncate()

    def start_supabase_instance(self, debug=False) -> None:
        """Call this function to start a supabase database"""
        self.logger.info('Starting Supabase')

        config_file_path = os.path.join(
            self.db_folder, 'supabase', 'config.toml')

        if not os.path.exists(config_file_path):
            self.initialize_supabase(config_file_path)

        self.stop_supabase_instance(no_log=True, debug=debug)
        if not debug:
            db_start_response = subprocess.run(
                'supabase start', shell=True, check=True, cwd=self.db_folder, capture_output=True, text=True)
        else:
            subprocess.run(
                'supabase start', shell=True, check=True, cwd=self.db_folder)
            self.logger.critical(
                "You have selected to launch Supabase in debug mode. Asaniczka module can't access any db functions. Please run without debug flag.")

        # extract supabase endpoints
        if not debug:
            db_start_response_lines = db_start_response.stdout.split('\n')
            for line in db_start_response_lines:
                if 'API URL' in line:
                    self.sb_api_url = line.split(':', maxsplit=1)[-1].strip()
                if 'DB URL' in line:
                    self.sb_db_url = line.split(':', maxsplit=1)[-1].strip()
                if 'Studio URL' in line:
                    self.sb_studio_url = line.split(
                        ':', maxsplit=1)[-1].strip()
                    self.logger.info(
                        f"Supabase STUDIO URL: {self.sb_studio_url}")
                if 'anon key' in line:
                    self.sb_anon_key = line.split(':', maxsplit=1)[-1].strip()

            self.db_backup_loop = True
            background_backup = threading.Thread(
                target=dbt.run_backup_every_hour, args=[self])
            background_backup.start()

    def stop_supabase_instance(self, no_log=False, debug=False) -> None:
        """Use this to stop any running supabase instances"""

        if not no_log:
            self.logger.info('Stopping any supabase instance')

        self.db_backup_loop = False  # stop backup if running
        try:
            if not debug:
                _ = subprocess.run(
                    'supabase stop', shell=True, check=True, cwd=self.db_folder, capture_output=True)
            else:
                subprocess.run(
                    'supabase stop', shell=True, check=True, cwd=self.db_folder, )

                self.sb_api_url = None
                self.sb_db_url = None
                self.sb_studio_url = None
                self.sb_anon_key = None

            if not no_log:
                self.logger.info(
                    "Supabase stopped sucessfully. Might take around 10 sec for bg tasks to finish")

        except subprocess.CalledProcessError as error:
            stderr_output = error.stderr.decode('utf-8')
            self.logger.critical(
                f"Unable to stop supabase. Error: {format_error(stderr_output)}")
            raise RuntimeError(
                "Unable to stop Supabase. Are you sure Docker is running? ") from error


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
        '%(asctime)s :  %(module)s  :   %(levelname)s   :   %(message)s')

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
