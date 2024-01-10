"""
This module provides functions to interact with a PostgreSQL database using the `psql` command.

## Functions:
- check_psql_installation()
- psql_subprocess_executor()
- get_sb_table_names()
- get_sb_column_details()
- run_sb_db_command()
- backup_sb_db()
- run_backup_every_6_hours()

## Classes
- SupabaseManager
"""

import subprocess
import os
from typing import Optional, Union
import logging
import datetime
import time
import threading
import random
import asaniczka


class SupabaseManager:
    """This is a wrapper for the supabase cli

    project is asaniczka.ProjectSetup
    """

    def __init__(self, project) -> None:
        if not project:
            raise AttributeError('Please send asaniczka.ProjectSetup')
        
        self.project = project
        self.sb_api_url = None
        self.sb_db_url = None
        self.sb_studio_url = None
        self.sb_anon_key = None
        self.db_backup_loop = False

    def check_supabase_cli_installation(self) -> None:
        """function checks if supabase cli is installed on the system"""
        # pylint: disable=bare-except
        try:
            _ = subprocess.run('supabase', shell=True,
                               check=True, capture_output=True)
            is_supabase_installed = True
        except:
            is_supabase_installed = False

        if not is_supabase_installed:
            self.project.logger.critical(
                "Asaniczka can't launch Supabase. You need to install supabase first. \nhttps://supabase.com/docs/guides/cli/getting-started")
            raise RuntimeError(
                "Asaniczka can't launch Supabase. You need to install supabase first. \nhttps://supabase.com/docs/guides/cli/getting-started")

    def initialize_supabase(self, config_file_path: os.PathLike) -> None:
        """Initalizes supabase for the current project"""
        self.project.logger.info("Creating supabase config")
        # initialize the project setup
        process = subprocess.Popen(['supabase', 'init'], stdout=subprocess.PIPE,
                                   stdin=subprocess.PIPE, stderr=subprocess.PIPE, cwd=self.project.db_folder)

        _ = process.communicate(input=b'n\n')

        # replace standard supabase ports with random ports to avoid clashes with other db instances
        with open(config_file_path, 'r+', encoding='utf-8') as config_file:
            lines = config_file.readlines()
            lines = [line.strip() for line in lines]
            lines = [line.replace(
                'project_id = "databases"', f'project_id = "{self.project.project_name}"') for line in lines]
            ports_to_replace = [54320,  54321, 54322,
                                54323, 54324, 54325, 54326,  54327, 54328,
                                54329, 54330]

            new_port_start = random.randint(20000, 50000)
            self.project.logger.debug(
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
        self.project.logger.info('Starting Supabase')

        config_file_path = os.path.join(
            self.project.db_folder, 'supabase', 'config.toml')

        if not os.path.exists(config_file_path):
            self.initialize_supabase(config_file_path)

        self.stop_supabase_instance(no_log=True, debug=debug)
        if not debug:
            try:
                db_start_response = subprocess.run(
                    'supabase start',
                    shell=True,
                    check=True,
                    cwd=self.project.db_folder,
                    capture_output=True,
                    text=True)
            except subprocess.CalledProcessError as error:
                error_message = error.stderr
                self.project.logger.critical(
                    f"Error when starting db: {asaniczka.format_error(error_message)}")
                raise RuntimeError("Error when starting db") from error
        else:
            subprocess.run(
                'supabase start', shell=True, check=True, cwd=self.project.db_folder, text=True)

            self.project.logger.critical(
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
                    self.project.logger.info(
                        f"Supabase STUDIO URL: {self.sb_studio_url}")
                if 'anon key' in line:
                    self.sb_anon_key = line.split(':', maxsplit=1)[-1].strip()

            self.db_backup_loop = True
            background_backup = threading.Thread(
                target=run_backup_every_hour, args=[self])
            background_backup.start()

    def stop_supabase_instance(self, no_log=False, debug=False) -> None:
        """Use this to stop any running supabase instances"""

        if not no_log:
            self.project.logger.info('Stopping any supabase instance')

        self.db_backup_loop = False  # stop backup if running
        try:
            if not debug:
                _ = subprocess.run(
                    'supabase stop',
                    shell=True,
                    check=True,
                    cwd=self.project.db_folder,
                    capture_output=True)
            else:
                subprocess.run(
                    'supabase stop', shell=True, check=True, cwd=self.project.db_folder, )

            self.sb_api_url = None
            self.sb_db_url = None
            self.sb_studio_url = None
            self.sb_anon_key = None

            if not no_log:
                self.project.logger.info(
                    "Supabase stopped sucessfully. Might take around 10 sec for bg tasks to finish")

        except subprocess.CalledProcessError as error:
            stderr_output = error.stderr.decode('utf-8')
            self.project.logger.critical(
                f"Unable to stop supabase. Error: {asaniczka.format_error(stderr_output)}")
            raise RuntimeError(
                "Unable to stop Supabase. Are you sure Docker is running?") from error


def check_psql_installation(logger: Optional[Union[None, logging.Logger]] = None) -> None:
    """
    Default checker to see if psql is installed.

    Args:
        `logger`: The logger object to use for logging. Defaults to None.

    Returns:
        `None`

    Raises:
        `RuntimeError`: If psql is not installed.

    """

    try:

        _ = subprocess.run("psql --version", shell=True,
                           check=True, capture_output=True, text=True)

    except subprocess.CalledProcessError as error:
        stderr = error.stderr
        if logger:
            logger.critical(
                f"Can't find psql. Do you have it installed? {asaniczka.format_error(stderr)}")
        raise RuntimeError(
            "Can't find psql. Do you have it installed? \nRun `sudo apt install postgresql-client-15`") from error


def psql_subprocess_executor(command: str, db_url: str) -> subprocess.CompletedProcess:
    """
    General try-except wrapper for executing psql commands via subprocess.

    Args:
        `command`: The psql command to be executed.
        `db_url`: The database URL.

    Returns:
        `subprocess.CompletedProcess`: The completed subprocess information.

    """

    psql_command = f'psql "{db_url}" -c "{command}"'

    # pylint:disable=subprocess-run-check
    completed_process = subprocess.run(
        psql_command, shell=True, capture_output=True, text=True)

    return completed_process


def get_sb_table_names(project=None,
                       db_url: Optional[Union[str, None]] = None,
                       logger: Optional[Union[logging.Logger, None]] = None,
                       make_list=False) -> str | list:
    """
    Get a list of all tables inside the database.

    Must send either `project` or `db_url and logger`

    Args:
        `project (asaniczka.ProjectSetup | None)`: The project setup object. Defaults to None.
        `db_url`: The database URL. Defaults to None.
        `logger`: The logger object to use for logging. Defaults to None.
        `make_list`: Whether to return the table names as a list or a string. Defaults to False.

    Returns:
        `str | list`: The table names.

    Raises:
        `AttributeError`: If db_url is not provided.
        `RuntimeError`: If the psql subprocess returns a non-zero exit code.

    """

    if project:
        logger = project.logger
        db_url = project.sb_db_url

    if not db_url:
        if logger:
            logger.critical(
                "You didn't send a db_url. By get_all_table_names()")
        raise AttributeError("You didn't send a db_url")

    check_psql_installation(logger)

    if make_list:
        command = "SELECT array_agg(table_name) FROM information_schema.tables WHERE table_schema = 'public';"
    else:
        command = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"

    completed_process = psql_subprocess_executor(command, db_url)

    if completed_process.returncode != 0:
        if logger:
            logger.error(
                f"Subprocess returned non-zero exist: {asaniczka.format_error(completed_process.stderr)}")

        raise RuntimeError(
            f'Subprocess returned non-zero exist: {asaniczka.format_error(completed_process.stderr)}')

    if make_list:
        return_bundle = completed_process.stdout.split(
            '{')[-1].split('}')[0].split(',')
    else:
        return_bundle = completed_process.stdout

    return return_bundle


def get_sb_column_details(table: str,
                          project=None,
                          db_url: Optional[Union[str, None]] = None,
                          logger: Optional[Union[logging.Logger, None]] = None) -> str:
    """
    Query Column names and data types of the provided table.

    Must send either `project` or `db_url and logger`

    Args:
        `table`: The name of the table.
        `project (asaniczka.ProjectSetup, None)`: A project setup instance (optional).
        `db_url`: The database URL (optional).
        `logger`: A logger instance (optional).

    Returns:
        `str`: The column details of the specified table.

    Raises:
        `AttributeError`: If no database URL is provided.
        `RuntimeError`: If the subprocess returns a non-zero exit code.
    """

    if project:
        logger = project.logger
        db_url = project.sb_db_url

    if not db_url:
        if logger:
            logger.critical(
                "You didn't send a db_url. By get_all_table_names()")
        raise AttributeError("You didn't send a db_url")

    check_psql_installation(logger)

    command = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table}';"

    completed_process = psql_subprocess_executor(command, db_url)

    if completed_process.returncode != 0:
        if logger:
            logger.error(
                f"Subprocess returned non-zero exist: {asaniczka.format_error(completed_process.stderr)}")

        raise RuntimeError(
            f'Subprocess returned non-zero exist: {asaniczka.format_error(completed_process.stderr)}')

    return_bundle = completed_process.stdout

    return return_bundle


def run_sb_db_command(command: str,
                      project=None,
                      db_url: Optional[Union[str, None]] = None,
                      logger: Optional[Union[logging.Logger, None]] = None) -> str | None:
    """
    Create a table on the Supabase database using psql.

    Must send either `project` or `db_url and logger`

    Args:
        `command`: The psql command to be executed.
        `project (asaniczka.ProjectSetup, None)`: A project setup instance (optional).
        `db_url`: The database URL (optional).
        `logger`: A logger instance (optional).

    Returns:
        `str | None`: The output of the command execution.

    Raises:
        `AttributeError`: If no database URL is provided.
        `RuntimeError`: If the subprocess returns a non-zero exit code.
    """

    if project:
        logger = project.logger
        db_url = project.sb_db_url

    if not db_url:
        if logger:
            logger.critical("You didn't send a db_url. By create_sb_table()")
        raise AttributeError("You didn't send a db_url")

    check_psql_installation(logger)

    completed_process = psql_subprocess_executor(command, db_url)

    if completed_process.returncode != 0:
        if logger:
            logger.error(
                f"Subprocess returned non-zero exist: {asaniczka.format_error(completed_process.stderr)}")

        raise RuntimeError(
            f'Subprocess returned non-zero exist: {asaniczka.format_error(completed_process.stderr)}')

    print(completed_process.stdout)


def backup_sb_db(project=None,
                 db_url: Optional[Union[str, None]] = None,
                 dest_folder: Optional[Union[os.PathLike, None]] = None,
                 logger: Optional[Union[None, logging.Logger]] = None) -> None:
    """
    Creates a backup of the database to the given folder.

    Must send either `asaniczka.ProjectSetup` or `db_url` and `dest_folder`.

    Args:
        `project (asaniczka.ProjectSetup, None)`: A project setup instance (optional).
        `db_url`: The database URL (optional).
        `dest_folder`: The destination folder to store the backup (optional).
        `logger`: A logger instance (optional).

    Raises:
        `AttributeError`: If no database URL is provided.

    """

    if project:
        logger = project.logger
        db_url = project.sb_db_url

    if not db_url:
        if logger:
            logger.critical("You didn't send a db_url. By backup_sb_db()")
        raise AttributeError("You didn't send a db_url")

    check_psql_installation(logger)

    time_right_now = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

    if project:
        dest_folder = os.path.join(project.project.db_folder, 'backups')
    os.makedirs(dest_folder, exist_ok=True)

    schema_path = os.path.join(dest_folder, f"{time_right_now}_schema.sql")
    roles_path = os.path.join(dest_folder, f"{time_right_now}_roles.sql")
    data_path = os.path.join(dest_folder, f"{time_right_now}_data.sql")

    command = f"supabase db dump --db-url '{db_url}' -f '{schema_path}';supabase db dump --db-url '{db_url}' -f '{roles_path}' --role-only;supabase db dump --db-url '{db_url}' -f '{data_path}' --data-only;"

    # pylint: disable=subprocess-run-check
    completed_process = subprocess.run(
        command, shell=True, text=True, capture_output=True)

    if completed_process.returncode != 0:
        if logger:
            logger.error(
                f"Error when backing up database: {asaniczka.format_error(completed_process.stdout)}")
        else:
            print(
                f"Error when backing up database: {asaniczka.format_error(completed_process.stdout)}")

    if logger:
        logger.info('Back up completed!')


def run_backup_every_hour(project) -> None:
    """
    Background task to run database backup every 6 hours.

    Args:
        `project (asaniczka.ProjectSetup)`: A project setup instance.

    """

    def do_sleep(time_to_sleep: int) -> None:
        time.sleep(time_to_sleep)

    # time_to_sleep = 30*60  # sleep for 30 mins before starting
    time_to_sleep = 0
    while project.db_backup_loop:
        if time_to_sleep < 1:
            project.logger.info('Backing up the database')
            backup_sb_db(project)
            time_to_sleep = 60*60

        do_sleep(10)  # sleep in 10 sec intervals
        time_to_sleep -= 10
