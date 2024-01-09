import subprocess
import os
from typing import Optional, Union
import asaniczka


def create_sb_table(command: str,
                    project: Optional[Union[asaniczka.ProjectSetup, None]] = None,
                    db_url: Optional[Union[str, None]] = None,
                    logger: Optional[Union[str, None]] = None) -> None:
    """Create a table on the supabase db using psql

    Send `db_url` and `logger` or `asaniczka.ProjectSetup`
    """

    if project:
        logger = project.logger
        db_url = project.sb_db_url

    if not db_url:
        if logger:
            logger.critical("You didn't send a db_url. By create_sb_table()")
        raise AttributeError("You didn't send a db_url")

    # check if psql is installed
    try:

        _ = subprocess.run("psql --version", shell=True,
                                check=True, capture_output=True, text=True)

    except subprocess.CalledProcessError as error:
        stderr = error.stderr
        if logger:
            logger.critical(
                f"Can't find psql. Do you have it installed? {asaniczka.format_error(stderr)}")
        raise RuntimeError("Can't find psql. Do you have it installed? \nRun `sudo apt install postgresql`") from error


create_sb_table("", db_url=1)
