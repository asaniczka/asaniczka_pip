import subprocess
import os
from typing import Optional, Union
import logging
import asaniczka


def check_for_psql_installation(logger: Optional[Union[None, logging.Logger]] = None) -> None:
    """Default checker to see if psql is installed"""

    try:

        _ = subprocess.run("psql --version", shell=True,
                           check=True, capture_output=True, text=True)

    except subprocess.CalledProcessError as error:
        stderr = error.stderr
        if logger:
            logger.critical(
                f"Can't find psql. Do you have it installed? {asaniczka.format_error(stderr)}")
        raise RuntimeError(
            "Can't find psql. Do you have it installed? \nRun `sudo apt install postgresql`") from error


def psql_subprocess_executor(command: str, db_url: str) -> subprocess.CompletedProcess:
    """General try except wrapper for psql commands via subprocess"""

    psql_command = f'psql "{db_url}" -c "{command}"'

    # pylint:disable=subprocess-run-check
    completed_process = subprocess.run(
        psql_command, shell=True, capture_output=True, text=True)

    return completed_process


def get_all_table_names(project: Optional[Union[asaniczka.ProjectSetup, None]] = None,
                        db_url: Optional[Union[str, None]] = None,
                        logger: Optional[Union[logging.Logger, None]] = None,
                        make_list=False) -> str | list:
    """get a list of all tables inside the database"""

    if project:
        logger = project.logger
        db_url = project.sb_db_url

    if not db_url:
        if logger:
            logger.critical(
                "You didn't send a db_url. By get_all_table_names()")
        raise AttributeError("You didn't send a db_url")

    check_for_psql_installation(logger)

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


def get_column_details(table: str,
                       project: Optional[Union[asaniczka.ProjectSetup, None]] = None,
                       db_url: Optional[Union[str, None]] = None,
                       logger: Optional[Union[logging.Logger, None]] = None) -> str:
    """Query Column names and data types of the provided table"""

    if project:
        logger = project.logger
        db_url = project.sb_db_url

    if not db_url:
        if logger:
            logger.critical(
                "You didn't send a db_url. By get_all_table_names()")
        raise AttributeError("You didn't send a db_url")

    check_for_psql_installation(logger)

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


def run_sbdb_command(command: str,
                     project: Optional[Union[asaniczka.ProjectSetup, None]] = None,
                     db_url: Optional[Union[str, None]] = None,
                     logger: Optional[Union[logging.Logger, None]] = None) -> str | None:
    """Create a table on the supabase db using psql

    Send `db_url` and `logger` or `asaniczka.ProjectSetup`

    Wrap and string inside single quoes `''` as `""` are used by subprocess
    """

    if project:
        logger = project.logger
        db_url = project.sb_db_url

    if not db_url:
        if logger:
            logger.critical("You didn't send a db_url. By create_sb_table()")
        raise AttributeError("You didn't send a db_url")

    check_for_psql_installation(logger)

    completed_process = psql_subprocess_executor(command, db_url)

    if completed_process.returncode != 0:
        if logger:
            logger.error(
                f"Subprocess returned non-zero exist: {asaniczka.format_error(completed_process.stderr)}")

        raise RuntimeError(
            f'Subprocess returned non-zero exist: {asaniczka.format_error(completed_process.stderr)}')

    print(completed_process.stdout)


# PGPASSWORD="your_password" psql -U username -h hostname -d dbname
    #
# PGPASSWORD="postgres" psql -U postgres -h hostname -d dbname
DB_URL = 'postgresql://postgres:postgres@127.0.0.1:25392/postgres'

# run_sb_command(
#     "CREATE TABLE authors (id SERIAL PRIMARY KEY, location TEXT NOT NULL);", db_url=DB_URL)
print(get_all_table_names(
    db_url=DB_URL, make_list=True))

print(get_column_details('users', db_url=DB_URL))
