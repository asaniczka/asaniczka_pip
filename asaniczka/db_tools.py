"""
This module provides functions to interact with a PostgreSQL database using the `psql` command.

Functions:
- check_psql_installation()
- psql_subprocess_executor()
- get_sb_table_names()
- get_sb_column_details()
- run_sb_db_command()
- backup_sb_db()
- run_backup_every_6_hours()
"""

import subprocess
import os
from typing import Optional, Union
import logging
import datetime
import time
import asaniczka.main as asaniczka


def check_psql_installation(logger: Optional[Union[None, logging.Logger]] = None) -> None:
    """
    Default checker to see if psql is installed.

    Args:
        `logger (None, logging.Logger)`: The logger object to use for logging. Defaults to None.

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
        `command (str)`: The psql command to be executed.
        `db_url (str)`: The database URL.

    Returns:
        `subprocess.CompletedProcess`: The completed subprocess information.

    """

    psql_command = f'psql "{db_url}" -c "{command}"'

    # pylint:disable=subprocess-run-check
    completed_process = subprocess.run(
        psql_command, shell=True, capture_output=True, text=True)

    return completed_process


def get_sb_table_names(project: Optional[Union[asaniczka.ProjectSetup, None]] = None,
                       db_url: Optional[Union[str, None]] = None,
                       logger: Optional[Union[logging.Logger, None]] = None,
                       make_list=False) -> str | list:
    """
    Get a list of all tables inside the database.

    Must send either `project` or `db_url and logger`

    Args:
        `project (asaniczka.ProjectSetup | None)`: The project setup object. Defaults to None.
        `db_url (str | None)`: The database URL. Defaults to None.
        `logger (logging.Logger | None)`: The logger object to use for logging. Defaults to None.
        `make_list (bool)`: Whether to return the table names as a list or a string. Defaults to False.

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
                          project: Optional[Union[asaniczka.ProjectSetup, None]] = None,
                          db_url: Optional[Union[str, None]] = None,
                          logger: Optional[Union[logging.Logger, None]] = None) -> str:
    """
    Query Column names and data types of the provided table.

    Must send either `project` or `db_url and logger`

    Args:
        `table (str)`: The name of the table.
        `project (asaniczka.ProjectSetup, None)`: A project setup instance (optional).
        `db_url (str, None)`: The database URL (optional).
        `logger (logging.Logger, None)`: A logger instance (optional).

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
                      project: Optional[Union[asaniczka.ProjectSetup, None]] = None,
                      db_url: Optional[Union[str, None]] = None,
                      logger: Optional[Union[logging.Logger, None]] = None) -> str | None:
    """
    Create a table on the Supabase database using psql.

    Must send either `project` or `db_url and logger`

    Args:
        `command (str)`: The psql command to be executed.
        `project (asaniczka.ProjectSetup, None)`: A project setup instance (optional).
        `db_url (str, None)`: The database URL (optional).
        `logger (logging.Logger, None)`: A logger instance (optional).

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


def backup_sb_db(project: Optional[Union[asaniczka.ProjectSetup, None]] = None,
                 db_url: Optional[Union[str, None]] = None,
                 dest_folder: Optional[Union[os.PathLike, None]] = None,
                 logger: Optional[Union[None, logging.Logger]] = None) -> None:
    """
    Creates a backup of the database to the given folder.

    Must send either `asaniczka.ProjectSetup` or `db_url` and `dest_folder`.

    Args:
        `project (asaniczka.ProjectSetup, None)`: A project setup instance (optional).
        `db_url (str, None)`: The database URL (optional).
        `dest_folder (os.PathLike, None)`: The destination folder to store the backup (optional).
        `logger (None, logging.Logger)`: A logger instance (optional).

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
        dest_folder = os.path.join(project.db_folder, 'backups')
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


def run_backup_every_hour(project: asaniczka.ProjectSetup) -> None:
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
        print(time_to_sleep)
