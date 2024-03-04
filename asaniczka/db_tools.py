"""
### Introduction
This module contains useful DB functions, which helps handle database instances for a project.

### High-level overview
- **SupabaseManager Class**:
    - Helps in managing Supabase instances for the current project.
    - It can start, stop, and initialize Supabase databases.
- **Methods**:
    - `check_supabase_cli_installation`: Checks if Supabase CLI is installed.
    - `initialize_supabase`: Initializes Supabase for the project.
    - `start_supabase_instance`: Starts a Supabase database instance.
    - `stop_supabase_instance`: Stops any running Supabase instances.
- **Other Functions**:
    - `check_psql_installation`: Checks if PostgreSQL command-line tool is installed.
    - `get_table_names_psql`: Retrieves a list of all tables inside the database.
    - `get_column_details_psql`: Queries column names and data types of a table.
- **Script Flow**:
    - It runs commands to check installations, start and stop database instances, and perform backups at specified intervals.
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

import asaniczka.main as asaniczka


class SupabaseManager:
    """
    This class is a wrapper for the Supabase CLI tool for managing Supabase instances.

    ### Attributes:
    - `project`: Instance of `asaniczka.ProjectSetup` for the current project.
    - `logger`: Logger object for logging messages.
    - `sb_api_url`: String representing the Supabase API URL.
    - `sb_db_url`: String representing the Supabase database URL.
    - `sb_studio_url`: String representing the Supabase Studio URL.
    - `sb_anon_key`: String representing the Supabase anonymous key.
    - `db_backup_loop`: Boolean flag to control database backup loop.
    - `is_db_backup_running`: Boolean flag to indicate if a database backup is currently running.

    ### Methods:
    - `__init__`: Initializes the SupabaseManager instance with project details.
    - `check_supabase_cli_installation`: Checks if the Supabase CLI tool is installed on the system.
    - `initialize_supabase`: Initializes Supabase for the current project by creating the Supabase config.
    - `start_supabase_instance`: Starts a Supabase database instance with options for debug mode and backup.
    - `stop_supabase_instance`: Stops any running Supabase instances with options for logging, debug mode, and backup.

    """

    def __init__(
        self,
        project,
        sb_api_url: str = None,
        sb_anon_key: str = None,
        sb_db_url: str = None,
    ) -> None:
        if not project:
            raise AttributeError("Please send asaniczka.ProjectSetup")

        self.project = project
        self.logger = self.project.logger
        self.sb_api_url = sb_api_url
        self.sb_db_url = sb_db_url
        self.sb_studio_url = None
        self.sb_anon_key = sb_anon_key
        self.db_backup_loop = False
        self.is_db_backup_running = False

    def check_supabase_cli_installation(self) -> None:
        """
        Function checks if supabase cli is installed on the system

        ### Responsibility:
        - Check if the Supabase CLI is installed on the system.

        ### Args:
        &nbsp;&nbsp; - `self`: Refers to an instance of a class.

        ### Returns:
        - None

        ### Raises:
        - `RuntimeError`: If Supabase CLI is not installed on the system.
        """
        # pylint: disable=bare-except
        try:
            _ = subprocess.run("supabase", shell=True, check=True, capture_output=True)
            is_supabase_installed = True
        except:
            is_supabase_installed = False

        if not is_supabase_installed:
            self.project.logger.critical(
                "Asaniczka can't launch Supabase. You need to install supabase first. \nhttps://supabase.com/docs/guides/cli/getting-started"
            )
            raise RuntimeError(
                "Asaniczka can't launch Supabase. You need to install supabase first. \nhttps://supabase.com/docs/guides/cli/getting-started"
            )

    def initialize_supabase(self, config_file_path: os.PathLike) -> None:
        """
        Initalizes supabase for the current project

        ### Responsibility:
        - Initializes Supabase for the current project by creating the Supabase config.
        - Replaces standard Supabase ports with random ports to avoid clashes with other database instances.

        ### Args:
        - `self`: Refers to an instance of a class.
        - `config_file_path`: Path to the Supabase config file.

        ### Returns:
        - None

        ### Raises:
        - No explicit errors raised by this function.
        """
        self.project.logger.info("Creating supabase config")
        # initialize the project setup
        process = subprocess.Popen(
            ["supabase", "init"],
            stdout=subprocess.PIPE,
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=self.project.db_folder,
        )

        _ = process.communicate(input=b"n\n")

        # replace standard supabase ports with random ports to avoid clashes with other db instances
        with open(config_file_path, "r+", encoding="utf-8") as config_file:
            lines = config_file.readlines()
            lines = [line.strip() for line in lines]
            lines = [
                line.replace(
                    'project_id = "databases"',
                    f'project_id = "{self.project.project_name}"',
                )
                for line in lines
            ]
            ports_to_replace = [
                54320,
                54321,
                54322,
                54323,
                54324,
                54325,
                54326,
                54327,
                54328,
                54329,
                54330,
            ]

            new_port_start = random.randint(20000, 50000)
            self.project.logger.debug(f"supabase port start value is: {new_port_start}")
            new_ports_list = []
            for idx, port in enumerate(ports_to_replace):
                new_ports_list.append(new_port_start + idx)

            modified_lines = []
            for line in lines:
                for idx, port in enumerate(ports_to_replace):
                    line = line.replace(str(port), str(new_ports_list[idx]))
                modified_lines.append(line)

            config_file.seek(0)
            config_file.write("\n".join(modified_lines))
            config_file.truncate()

    def start_supabase_instance(self, debug=False) -> None:
        """
        Call this function to start a Supabase database

        ### Responsibility:
        - Starts a Supabase database instance for the current project.
        - Initializes Supabase if the config file does not exist.
        - Handles starting the Supabase instance with or without debug mode.
        - Extracts Supabase endpoints and logs them.
        - Initiates a thread for running database backups every hour if not in debug mode.

        ### Args:
        - `self`: Refers to an instance of a class.
        - `debug (optional)`: Boolean flag to specify whether to run in debug mode. Default is False.

        ### Returns:
        - None

        ### Raises:
        - `RuntimeError`: If there is an error when starting the database.
        """

        self.project.logger.info("Starting Supabase")

        config_file_path = os.path.join(
            self.project.db_folder, "supabase", "config.toml"
        )

        if not os.path.exists(config_file_path):
            self.initialize_supabase(config_file_path)

        self.stop_supabase_instance(no_log=True, debug=debug, backup=False)
        if not debug:
            try:
                db_start_response = subprocess.run(
                    "supabase start",
                    shell=True,
                    check=True,
                    cwd=self.project.db_folder,
                    capture_output=True,
                    text=True,
                )
            except subprocess.CalledProcessError as error:
                error_message = error.stderr
                self.project.logger.critical(
                    f"Error when starting db: {asaniczka.format_error(error_message)}"
                )
                raise RuntimeError("Error when starting db") from error
        else:
            subprocess.run(
                "supabase start",
                shell=True,
                check=True,
                cwd=self.project.db_folder,
                text=True,
            )

            self.project.logger.critical(
                "You have selected to launch Supabase in debug mode. Asaniczka module can't access any db functions. Please run without debug flag."
            )

        # extract supabase endpoints
        if not debug:
            db_start_response_lines = db_start_response.stdout.split("\n")
            for line in db_start_response_lines:
                if "API URL" in line:
                    self.sb_api_url = line.split(":", maxsplit=1)[-1].strip()
                if "DB URL" in line:
                    self.sb_db_url = line.split(":", maxsplit=1)[-1].strip()
                if "Studio URL" in line:
                    self.sb_studio_url = line.split(":", maxsplit=1)[-1].strip()
                if "anon key" in line:
                    self.sb_anon_key = line.split(":", maxsplit=1)[-1].strip()

            items_to_log = {
                "API URL": self.sb_api_url,
                "DB URL": self.sb_db_url,
                "Studio URL": self.sb_studio_url,
                "anon key": self.sb_anon_key,
            }

            for key, value in items_to_log.items():
                self.project.logger.info(f"Supabase {key}: {value}")

            self.db_backup_loop = True
            background_backup = threading.Thread(
                target=run_backup_every_hour, args=[self]
            )
            background_backup.start()

        self.logger.info("Supabase started sucessfully!")

    def stop_supabase_instance(self, no_log=False, debug=False, backup=True) -> None:
        """
        Use this function to stop any running Supabase instances

        ### Responsibility:
        - Stops any running Supabase instances for the current project.
        - Stops the backup loop if active.
        - Waits for the current backup to finish before stopping.
        - Restarts Supabase with or without running a backup before stopping.
        - Clears Supabase endpoints after stopping.
        - Logs the successful stop of Supabase instance.

        ### Args:
        - `self`: Refers to an instance of a class.
        - `no_log (optional)`: Boolean flag to specify if logging should be skipped. Default is False.
        - `debug (optional)`: Boolean flag to specify whether to stop in debug mode. Default is False.
        - `backup (optional)`: Boolean flag to specify whether to run a backup before stopping. Default is True.

        ### Returns:
        - None

        ### Raises:
        - `RuntimeError`: If unable to stop Supabase due to Docker not running or other reasons.
        """

        if not no_log:
            self.project.logger.info("Stopping any supabase instance")

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
                    "supabase stop",
                    shell=True,
                    check=True,
                    cwd=self.project.db_folder,
                    capture_output=True,
                )
            else:
                subprocess.run(
                    "supabase stop",
                    shell=True,
                    check=True,
                    cwd=self.project.db_folder,
                )

            self.sb_api_url = None
            self.sb_db_url = None
            self.sb_studio_url = None
            self.sb_anon_key = None

            if not no_log:
                self.project.logger.info(
                    "Supabase stopped sucessfully. Might take around 10 sec for bg tasks to finish"
                )

        except subprocess.CalledProcessError as error:
            stderr_output = error.stderr.decode("utf-8")
            self.project.logger.critical(
                f"Unable to stop supabase. Error: {asaniczka.format_error(stderr_output)}"
            )
            raise RuntimeError(
                "Unable to stop Supabase. Are you sure Docker is running?"
            ) from error


def check_psql_installation(
    logger: Optional[Union[None, logging.Logger]] = None
) -> None:
    """
    Default checker to see if psql (PostgreSQL) is installed.

    ### Responsibility:
    - Checks if the psql command-line tool for PostgreSQL is installed on the system.

    ### Args:
    - `logger`: Optional parameter for passing a logger object to log messages. Defaults to None.

    ### Returns:
    - None

    ### Raises:
    - `RuntimeError`: If the psql command-line tool is not installed on the system.
    """

    try:
        _ = subprocess.run(
            "psql --version", shell=True, check=True, capture_output=True, text=True
        )

    except subprocess.CalledProcessError as error:
        stderr = error.stderr
        if logger:
            logger.critical(
                f"Can't find psql. Do you have it installed? {asaniczka.format_error(stderr)}"
            )
        raise RuntimeError(
            "Can't find psql. Do you have it installed? \nRun `sudo apt install postgresql-client-15`"
        ) from error


def psql_subprocess_executor(command: str, db_url: str) -> subprocess.CompletedProcess:
    """
    General try-except wrapper for executing psql commands via subprocess.

    ### Responsibility:
    - Executes a psql command on a specified database URL using subprocess.

    ### Args:
    - `command`: The psql command to be executed.
    - `db_url`: The database URL where the command will be executed.

    ### Returns:
    - `subprocess.CompletedProcess`: Information about the completed subprocess execution.

    """

    psql_command = f'psql "{db_url}" -c "{command}"'

    # pylint:disable=subprocess-run-check
    completed_process = subprocess.run(
        psql_command, shell=True, capture_output=True, text=True
    )

    return completed_process


def get_table_names_psql(
    sb_manager=None,
    db_url: Optional[Union[str, None]] = None,
    logger: Optional[Union[logging.Logger, None]] = None,
    make_list=False,
) -> str | list:
    """
    Get a list of all tables inside the database.

    Must send either `sb_manager` or `db_url and logger`

    ### Responsibility:
    - Retrieves a list of table names from a specified database using psql commands.

    ### Args:
    - `sb_manager (dbt.SupabaseManager | None)`: The SupabaseManager instance. Defaults to None.
    - `db_url`: The database URL where the tables exist.
    - `logger`: The logger object to use for logging. Defaults to None.
    - `make_list`: Whether to return the table names as a list or a string. Defaults to False.

    ### Returns:
    - `str | list`: The table names.

    ### Raises:
    - `AttributeError`: If db_url is not provided.
    - `RuntimeError`: If the psql subprocess returns a non-zero exit code.

    """

    if sb_manager:
        logger = sb_manager.logger
        db_url = sb_manager.sb_db_url

    if not db_url:
        if logger:
            logger.critical("You didn't send a db_url. By get_all_table_names()")
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
                f"Subprocess returned non-zero exist: {asaniczka.format_error(completed_process.stderr)}"
            )

        raise RuntimeError(
            f"Subprocess returned non-zero exist: {asaniczka.format_error(completed_process.stderr)}"
        )

    if make_list:
        return_bundle = completed_process.stdout.split("{")[-1].split("}")[0].split(",")
    else:
        return_bundle = completed_process.stdout

    return return_bundle


def get_column_details_psql(
    table: str,
    sb_manager=None,
    db_url: Optional[Union[str, None]] = None,
    logger: Optional[Union[logging.Logger, None]] = None,
) -> str:
    """
    Query Column names and data types of the provided table.

    Must send either `sb_manager` or `db_url and logger`

    ### Responsibility:
    - Retrieves column names, data types, defaults, and nullability of a specific table from a database using psql commands.

    ### Args:
    - `table`: The name of the table for which column details are to be queried.
    - `sb_manager (dbt.SupabaseManaager, None)`: A SupabaseManaager instance (optional).
    - `db_url`: The database URL where the table is located (optional).
    - `logger`: A logger instance for logging information (optional).

    ### Returns:
    - `str`: The column details of the specified table.

    ### Raises:
    - `AttributeError`: If no database URL is provided.
    - `RuntimeError`: If the subprocess returns a non-zero exit code.
    """

    if sb_manager:
        logger = sb_manager.logger
        db_url = sb_manager.sb_db_url

    if not db_url:
        if logger:
            logger.critical("You didn't send a db_url. By get_all_table_names()")
        raise AttributeError("You didn't send a db_url")

    check_psql_installation(logger)

    command = f"SELECT column_name, data_type, column_default, is_nullable FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table}';"

    completed_process = psql_subprocess_executor(command, db_url)

    if completed_process.returncode != 0:
        if logger:
            logger.error(
                f"Subprocess returned non-zero exist: {asaniczka.format_error(completed_process.stderr)}"
            )

        raise RuntimeError(
            f"Subprocess returned non-zero exist: {asaniczka.format_error(completed_process.stderr)}"
        )

    return_bundle = completed_process.stdout

    return return_bundle


def run_db_command_psql(
    command: str,
    sb_manager=None,
    db_url: Optional[Union[str, None]] = None,
    logger: Optional[Union[logging.Logger, None]] = None,
) -> str | None:
    """
    Create a table on the Supabase database using psql.

    Must send either `sb_manager` or `db_url and logger`

    ### Responsibility:
    - Executes a psql command to create a table in the specified database.

    ### Args:
    - `command`: The psql command to create the table.
    - `sb_manager (dbt.SupabaseManager, None)`: A SupabaseManager instance (optional).
    - `db_url`: The database URL where the table is to be created (optional).
    - `logger`: A logger instance for logging information (optional).

    ### Returns:
    - `str | None`: The output of the command execution.

    ### Raises:
    - `AttributeError`: If no database URL is provided.
    - `RuntimeError`: If the subprocess returns a non-zero exit code.
    """

    if sb_manager:
        logger = sb_manager.logger
        db_url = sb_manager.sb_db_url

    if not db_url:
        if logger:
            logger.critical("You didn't send a db_url. By run_db_command_psql()")
        raise AttributeError("You didn't send a db_url")

    check_psql_installation(logger)

    completed_process = psql_subprocess_executor(command, db_url)

    if completed_process.returncode != 0:
        if logger:
            logger.error(
                f"Subprocess returned non-zero exist: {asaniczka.format_error(completed_process.stderr)}"
            )

        raise RuntimeError(
            f"Subprocess returned non-zero exist: {asaniczka.format_error(completed_process.stderr)}"
        )

    return completed_process.stdout


def backup_db_psql(
    sb_manager=None,
    db_url: Optional[Union[str, None]] = None,
    dest_folder: Optional[Union[os.PathLike, None]] = None,
    logger: Optional[Union[None, logging.Logger]] = None,
) -> None:
    """
    Creates a backup of the database to the specified folder.

    Must send either `sb_manager` setup or `db_url` and `dest_folder`.

    ### Responsibility:
    - Initiates a database backup process to store the database schema, roles, and data in separate SQL files.

    ### Args:
    - `sb_manager (asaniczka.sb_managerSetup, None)`: A setup instance for managing the database backup process (optional).
    - `db_url`: The database URL for the database to be backed up (optional).
    - `dest_folder`: The destination folder where the backup files will be stored (optional).
    - `logger`: A logger instance for logging information (optional).

    ### Raises:
    - `AttributeError`: If no database URL is provided.
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
        logger.info("Backing up Database!")

    check_psql_installation(logger)

    time_right_now = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    if sb_manager:
        dest_folder = os.path.join(sb_manager.project.db_folder, "backups")
    os.makedirs(dest_folder, exist_ok=True)

    schema_path = os.path.join(dest_folder, f"{time_right_now}_schema.sql")
    roles_path = os.path.join(dest_folder, f"{time_right_now}_roles.sql")
    data_path = os.path.join(dest_folder, f"{time_right_now}_data.sql")

    command = f"supabase db dump --db-url '{db_url}' -f '{schema_path}';supabase db dump --db-url '{db_url}' -f '{roles_path}' --role-only;supabase db dump --db-url '{db_url}' -f '{data_path}' --data-only;"

    # pylint: disable=subprocess-run-check
    completed_process = subprocess.run(
        command, shell=True, text=True, capture_output=True
    )

    if completed_process.returncode != 0:
        if logger:
            logger.error(
                f"Error when backing up database: {asaniczka.format_error(completed_process.stdout)}"
            )
        else:
            print(
                f"Error when backing up database: {asaniczka.format_error(completed_process.stdout)}"
            )

    if logger:
        logger.info("Back up completed!")

    if sb_manager:
        sb_manager.is_db_backup_running = False


def run_backup_every_hour(sb_manager) -> None:
    """
    Runs a background task to execute database backup every 6 hours.

    ### Responsibility:
    - Periodically triggers the database backup process at specified intervals.

    ### Args:
    - `sb_manager (dbt.SupabaseManager)`: A SupabaseManager instance responsible for managing the database backup process.

    ### Returns:
    - `None`

    ### Raises:
    - No explicit errors raised within the function.
    """

    def do_sleep(time_to_sleep: int) -> None:
        time.sleep(time_to_sleep)

    time_to_sleep = 30 * 60  # sleep for 30 mins before starting

    while sb_manager.db_backup_loop:
        if time_to_sleep < 1:
            sb_manager.logger.info("Backing up the database")
            backup_db_psql(sb_manager=sb_manager)
            time_to_sleep = 60 * 60

        do_sleep(10)  # sleep in 10 sec intervals
        time_to_sleep -= 10
