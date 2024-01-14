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
import asyncio
import supabase
from supabase import Client, create_client
import asaniczka


class SupabaseManager:
    """This is a wrapper for the supabase cli

    project is asaniczka.ProjectSetup
    """

    def __init__(self, project, sb_api_url: str = None, sb_anon_key: str = None, sb_db_url: str = None) -> None:
        if not project:
            raise AttributeError('Please send asaniczka.ProjectSetup')

        self.project = project
        self.logger = self.project.logger
        self.sb_api_url = sb_api_url
        self.sb_db_url = sb_db_url
        self.sb_studio_url = None
        self.sb_anon_key = sb_anon_key
        self.db_backup_loop = False
        self.is_db_backup_running = False
        self.sb_client: Client = self.create_supabse_client()

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

        self.stop_supabase_instance(no_log=True, debug=debug, backup=False)
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
                if 'anon key' in line:
                    self.sb_anon_key = line.split(':', maxsplit=1)[-1].strip()

            items_to_log = {
                'API URL': self.sb_api_url,
                'DB URL': self.sb_db_url,
                'Studio URL': self.sb_studio_url,
                'anon key': self.sb_anon_key
            }

            for key, value in items_to_log.items():
                self.project.logger.info(
                    f"Supabase {key}: {value}")

            self.create_supabse_client()

            self.db_backup_loop = True
            background_backup = threading.Thread(
                target=run_backup_every_hour, args=[self])
            background_backup.start()

        self.logger.info('Supabase started sucessfully!')

    def stop_supabase_instance(self, no_log=False, debug=False, backup=True) -> None:
        """Use this to stop any running supabase instances

        Run with backup=False to stop without knowing any SB endpoints

        """

        if not no_log:
            self.project.logger.info('Stopping any supabase instance')

        self.db_backup_loop = False  # stop backup loop if active

        # if the db is already running a backup, wait for it to finish
        while self.is_db_backup_running:
            self.logger.info("Waiting for DB to finish it's current backup.")
            time.sleep(30)

        if backup:
            backup_db_psql(self)
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

    def create_supabse_client(self) -> supabase.Client:
        """Create a supabase client"""

        if not self.sb_anon_key or not self.sb_api_url:
            self.logger.warning(
                'Supabase client not created since no url or anon key')
            return

        self.sb_client = create_client(
            self.sb_api_url, self.sb_anon_key)

        self.logger.info("Supabase client created successfully")

    def insert_row_to_db(self,
                         data_dict: dict,
                         table_name: str,
                         return_minimal: Optional[bool] = True,
                         suppress_errors: Optional[bool] = True,
                         suppress_logs=False) -> None | supabase.PostgrestAPIResponse:
        """Function will run an insert query with the given data to the table via supabase api"""
        try:
            data = self.sb_client.table(table_name)\
                .insert(data_dict, returning='minimal')\
                .execute()
        # pylint:disable=broad-except
        except Exception as error:
            if not suppress_logs:
                self.logger.error(
                    f'Error when inserting to {table_name}:    {asaniczka.format_error(error)}')
            if not suppress_errors:
                raise RuntimeError(
                    f'Error when inserting to db: {asaniczka.format_error(error)}') from error

        if not return_minimal:
            return data
        return None

    async def async_insert_row_to_db(self, data_dict: dict,
                                     table_name: str,
                                     return_minimal: Optional[bool] = True,
                                     suppress_errors: Optional[bool] = True,
                                     suppress_logs=False) -> None | supabase.PostgrestAPIResponse:
        """Async version of insert_row_to_db. 
        YOu don't have to wait for a response if you don't want to"""

        return await asyncio.to_thread(self.insert_row_to_db,
                                       data_dict,
                                       table_name,
                                       return_minimal=return_minimal,
                                       suppress_errors=suppress_errors,
                                       suppress_logs=suppress_logs)


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


def get_table_names_psql(sb_manager=None,
                         db_url: Optional[Union[str, None]] = None,
                         logger: Optional[Union[logging.Logger, None]] = None,
                         make_list=False) -> str | list:
    """
    Get a list of all tables inside the database.

    Must send either `sb_manager` or `db_url and logger`

    Args:
        `sb_manager (dbt.SupabaseManager | None)`: The SupabaseManager instance. Defaults to None.
        `db_url`: The database URL. Defaults to None.
        `logger`: The logger object to use for logging. Defaults to None.
        `make_list`: Whether to return the table names as a list or a string. Defaults to False.

    Returns:
        `str | list`: The table names.

    Raises:
        `AttributeError`: If db_url is not provided.
        `RuntimeError`: If the psql subprocess returns a non-zero exit code.

    """

    if sb_manager:
        logger = sb_manager.logger
        db_url = sb_manager.sb_db_url

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


def get_column_details_psql(table: str,
                            sb_manager=None,
                            db_url: Optional[Union[str, None]] = None,
                            logger: Optional[Union[logging.Logger, None]] = None) -> str:
    """
    Query Column names and data types of the provided table.

    Must send either `sb_manager` or `db_url and logger`

    Args:
        `table`: The name of the table.
        `sb_manager (dbt.SupabaseManaager, None)`: A SupabaseManaager instance (optional).
        `db_url`: The database URL (optional).
        `logger`: A logger instance (optional).

    Returns:
        `str`: The column details of the specified table.

    Raises:
        `AttributeError`: If no database URL is provided.
        `RuntimeError`: If the subprocess returns a non-zero exit code.
    """

    if sb_manager:
        logger = sb_manager.logger
        db_url = sb_manager.sb_db_url

    if not db_url:
        if logger:
            logger.critical(
                "You didn't send a db_url. By get_all_table_names()")
        raise AttributeError("You didn't send a db_url")

    check_psql_installation(logger)

    command = f"SELECT column_name, data_type, column_default, is_nullable FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table}';"

    completed_process = psql_subprocess_executor(command, db_url)

    if completed_process.returncode != 0:
        if logger:
            logger.error(
                f"Subprocess returned non-zero exist: {asaniczka.format_error(completed_process.stderr)}")

        raise RuntimeError(
            f'Subprocess returned non-zero exist: {asaniczka.format_error(completed_process.stderr)}')

    return_bundle = completed_process.stdout

    return return_bundle


def run_db_command_psql(command: str,
                        sb_manager=None,
                        db_url: Optional[Union[str, None]] = None,
                        logger: Optional[Union[logging.Logger, None]] = None) -> str | None:
    """
    Create a table on the Supabase database using psql.

    Must send either `sb_manager` or `db_url and logger`

    Args:
        `command`: The psql command to be executed.
        `sb_manager (dbt.SupabaseManager, None)`: A SupabaseManager instance (optional).
        `db_url`: The database URL (optional).
        `logger`: A logger instance (optional).

    Returns:
        `str | None`: The output of the command execution.

    Raises:
        `AttributeError`: If no database URL is provided.
        `RuntimeError`: If the subprocess returns a non-zero exit code.
    """

    if sb_manager:
        logger = sb_manager.logger
        db_url = sb_manager.sb_db_url

    if not db_url:
        if logger:
            logger.critical(
                "You didn't send a db_url. By run_db_command_psql()")
        raise AttributeError("You didn't send a db_url")

    check_psql_installation(logger)

    completed_process = psql_subprocess_executor(command, db_url)

    if completed_process.returncode != 0:
        if logger:
            logger.error(
                f"Subprocess returned non-zero exist: {asaniczka.format_error(completed_process.stderr)}")

        raise RuntimeError(
            f'Subprocess returned non-zero exist: {asaniczka.format_error(completed_process.stderr)}')

    return completed_process.stdout


def backup_db_psql(sb_manager=None,
                   db_url: Optional[Union[str, None]] = None,
                   dest_folder: Optional[Union[os.PathLike, None]] = None,
                   logger: Optional[Union[None, logging.Logger]] = None) -> None:
    """
    Creates a backup of the database to the given folder.

    Must send either `asaniczka.sb_managerSetup` or `db_url` and `dest_folder`.

    Args:
        `sb_manager (asaniczka.sb_managerSetup, None)`: A sb_manager setup instance (optional).
        `db_url`: The database URL (optional).
        `dest_folder`: The destination folder to store the backup (optional).
        `logger`: A logger instance (optional).

    Raises:
        `AttributeError`: If no database URL is provided.

    """

    if sb_manager:
        logger = sb_manager.logger
        db_url = sb_manager.sb_db_url
        sb_manager.is_db_backup_running = True

    if not db_url:
        if logger:
            logger.critical("You didn't send a db_url. By backup_sb_db()")
        raise AttributeError("You didn't send a db_url")
    if logger:
        logger.info('Backing up Database!')

    check_psql_installation(logger)

    time_right_now = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

    if sb_manager:
        dest_folder = os.path.join(sb_manager.project.db_folder, 'backups')
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

    if sb_manager:
        sb_manager.is_db_backup_running = False


def run_backup_every_hour(sb_manager) -> None:
    """
    Background task to run database backup every 6 hours.

    Args:
        `sb_manager (dbt.SupabaseManager)`: A SupabaseManager instance.

    """

    def do_sleep(time_to_sleep: int) -> None:
        time.sleep(time_to_sleep)

    time_to_sleep = 30*60  # sleep for 30 mins before starting

    while sb_manager.db_backup_loop:
        if time_to_sleep < 1:
            sb_manager.logger.info('Backing up the database')
            backup_db_psql(sb_manager=sb_manager)
            time_to_sleep = 60*60

        do_sleep(10)  # sleep in 10 sec intervals
        time_to_sleep -= 10
