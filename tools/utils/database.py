import inspect
import os
import re
import shutil
import subprocess
import sys
import time

import importlib
import mariadb
from mariadb.constants import ERR, FIELD_TYPE
import pathlib
import types

# Add parent directory to module search path so that relative imports work when running the script directly
tools_path = os.path.abspath(
    os.path.dirname(os.path.dirname(inspect.getfile(inspect.currentframe())))
)
if tools_path not in sys.path:
    sys.path.insert(0, tools_path)

from utils.common import Common


class Database:
    """Database util class.

    Holds all database connection info and state. Performs operations on
    the database.

    Parameters
    ----------
    login : str
        The username used to authenticate with the database server
    password : str
        The password of the given user
    database : str
        Database (schema) name to use when connecting with the database
        server
    host : str
        The host name or IP address of the database server. If MariaDB
        Connector/Python was built with MariaDB Connector/C 3.3 it is also
        possible to provide a comma separated list of hosts for simple
        fail over in case of one or more hosts are not available.
    port : int
        Port number of the database server
    mysql_bin : str
        Path to mysql bin directory (mysql, mysqladmin, mysqldump)
    """

    # These are the 'protected' tables
    protected_tables = [
        "accounts",
        "accounts_banned",
        "auction_house_items",
        "auction_house",
        "char_blacklist",
        "char_chocobos",
        "char_effects",
        "char_equip",
        "char_equip_saved",
        "char_exp",
        "char_history",
        "char_inventory",
        "char_jobs",
        "char_job_points",
        "char_look",
        "char_merit",
        "char_pet",
        "char_points",
        "char_profile",
        "char_skills",
        "char_spells",
        "char_stats",
        "char_storage",
        "char_style",
        "char_unlocks",
        "char_vars",
        "chars",
        "conquest_system",
        "delivery_box",
        "ip_exceptions",
        "linkshells",
        "server_variables",
        "unity_system",
    ]

    try:
        connection = mariadb.connect(
            host=Common.settings["network"]["SQL_HOST"],
            port=int(Common.settings["network"]["SQL_PORT"]),
            user=Common.settings["network"]["SQL_LOGIN"],
            passwd=Common.settings["network"]["SQL_PASSWORD"],
        )
        cursor = connection.cursor()
    except mariadb.Error as err:
        if err.errno == ERR.ER_ACCESS_DENIED_ERROR:
            Common.print_red("Database access denied, check network.lua.")
        Common.close(-1)

    @classmethod
    def create(cls) -> bool:
        """Create database using mysqladmin.

        Returns
        -------
        subprocess.CompletedProcess
            The return value from subprocess.run(), representing a process
            that has finished
        """
        print(f'Creating database "{Common.settings["network"]["SQL_DATABASE"]}"...')
        Database.cursor.execute(
            f'DROP DATABASE IF EXISTS {Common.settings["network"]["SQL_DATABASE"]}'
        )
        Common.configs["db_ver"] = None
        errors = []
        result = subprocess.run(
            [
                f"{os.path.join(Common.configs['mysql_bin'], 'mysqladmin')}",
                f"-h{Common.settings['network']['SQL_HOST']}",
                f"-P{str(Common.settings['network']['SQL_PORT'])}",
                f"-u{Common.settings['network']['SQL_LOGIN']}",
                f"-p{Common.settings['network']['SQL_PASSWORD']}",
                "CREATE",
                Common.settings["network"]["SQL_DATABASE"],
            ],
            capture_output=True,
            text=True,
        )
        errors.extend(Database.fetch_errors(result))
        Database.connection.database = Common.settings["network"]["SQL_DATABASE"]
        import_files = [
            os.path.join(Common.root_path, "sql", f"{value}.sql")
            for value in Database.protected_tables
        ]
        import_files.extend(Database.fetch_files())
        for sql_file in import_files:
            sql_file = os.path.normpath(sql_file).replace("\\", "/")
            if os.path.exists(sql_file):
                print(f"Importing {sql_file}")
                errors.extend(Database.fetch_errors(Database.import_file(sql_file)))
                print(Common.LINE_UP, Common.ERASE_TO_END_OF_LINE, end="\r")
        if errors:
            print(f"Errors occurred while setting up the database.")
            for error in errors:
                Common.print_red(error)
            return False
        else:
            print(Common.LINE_UP, Common.ERASE_TO_END_OF_LINE, end="\r")
            Common.print_green(
                f'Database "{Common.settings["network"]["SQL_DATABASE"]}" setup complete.'
            )
            Common.configs["db_ver"] = Common.version
            Common.write_configs()
            return True

    @classmethod
    def backup(cls, out_dir: str, tables: list[str]) -> subprocess.CompletedProcess:
        """Backup the database using mysqldump.

        Parameters
        ----------
        out_dir : str
            Directory to save backup
        tables : list[str], optional
            If present, only backup listed tables

        Returns
        -------
        subprocess.CompletedProcess
            The return value from subprocess.run(), representing a process
            that has finished
        """
        dumpcmd = [
            f"{os.path.join(Common.configs['mysql_bin'], 'mysqldump')}",
            "--hex-blob",
            "--add-drop-trigger",
            f"-h{Common.settings['network']['SQL_HOST']}",
            f"-P{str(Common.settings['network']['SQL_PORT'])}",
            f"-u{Common.settings['network']['SQL_LOGIN']}",
            f"-p{Common.settings['network']['SQL_PASSWORD']}",
            Common.settings["network"]["SQL_DATABASE"],
        ]
        if tables:
            dumpcmd.extend(tables)
        outfile_path = os.path.normpath(
            os.path.join(
                out_dir,
                f"{Common.settings['network']['SQL_DATABASE']}-{time.strftime('%Y%m%d-%H%M%S')}-{'lite' if tables else Common.configs['db_ver'] if Common.configs['db_ver'] else 'full'}.sql",
            )
        ).replace("\\", "/")
        with open(outfile_path, "w") as outfile:
            result = subprocess.run(
                dumpcmd,
                stdout=outfile,
                stderr=subprocess.PIPE,
                text=True,
            )
        return result

    @classmethod
    def query(cls, query: str) -> subprocess.CompletedProcess:
        """Run a query using mysql bin.

        Parameters
        ----------
        query : str
            Query to execute

        Returns
        -------
        subprocess.CompletedProcess
            The return value from subprocess.run(), representing a process
            that has finished
        """
        result = subprocess.run(
            [
                f"{os.path.join(Common.configs['mysql_bin'], 'mysql')}",
                f"-h{Common.settings['network']['SQL_HOST']}",
                f"-P{str(Common.settings['network']['SQL_PORT'])}",
                f"-u{Common.settings['network']['SQL_LOGIN']}",
                f"-p{Common.settings['network']['SQL_PASSWORD']}",
                Common.settings["network"]["SQL_DATABASE"],
                f"-e {query}",
            ],
            capture_output=True,
            text=True,
        )
        return result

    @classmethod
    def update(cls):
        import_files = [
            os.path.join(Common.root_path, "sql", f"{value}.sql")
            for value in Database.protected_tables
            if value not in Database.check_missing(Database.protected_tables)
        ]
        import_files.extend(Database.fetch_files())

    @classmethod
    def import_file(cls, file: str) -> subprocess.CompletedProcess:
        """Commit the contents of a .sql file to the database.

        Parameters
        ----------
        file : str
            The path to the file to import

        Returns
        -------
        int
            The number of rows produced or affected
        """
        file.replace("\\", "/")
        query = f"SET autocommit=0; SET unique_checks=0; SET foreign_key_checks=0; SOURCE {file}; SET unique_checks=1; SET foreign_key_checks=1; COMMIT;"
        return Database.query(query)

    @classmethod
    def export_table(cls, table_name: str) -> None:
        """Replace values in .sql file with database values.

        Replaces values inside INSERT statements in .sql files with values
        from the corresponding row in the database.

        Parameters
        ----------
        table_name : str
            Name of table to export (table_name.sql is written to)
        """

        try:
            # Fetch all rows from the specified table
            Database.cursor.execute(f"SELECT * FROM {table_name};")
            rows = Database.cursor.fetchall()

            # Read the SQL file
            with open(
                f"{os.path.join(Common.root_path, 'sql', f'{table_name}.sql')}",
                "r",
                encoding="utf-8",
            ) as file:
                sql_lines = file.readlines()

            sql_variables = {}
            updated_lines = []
            row_index = 0

            # Iterate over the lines in the file
            for line in sql_lines:
                # Scan for variables
                if line.strip().startswith("SET @"):
                    parts = line.strip().split("=")
                    var_name = parts[0].split()[1]
                    var_value = parts[1].replace(";", "").split("--")[0].strip()
                    sql_variables[var_value] = var_name
                # Scan for INSERT
                lowercase_line = line.strip().lower()
                insert_start = re.match(
                    rf"insert into `{table_name}` values \(", lowercase_line
                )
                if insert_start:
                    # Build a string using the values pulled from the database
                    values = rows[row_index]
                    updated_values = []
                    for i, value in enumerate(values):
                        # NULL
                        if value is None:
                            updated_values.append("NULL")
                        # Binary
                        elif isinstance(value, bytes):
                            if len(value) == 0:
                                updated_values.append(f"''")
                            # npc_list name field is binary but should be decoded for the sql files
                            elif table_name == "npc_list" and i == 1:
                                text = True
                                for j in value:
                                    if j < 32 or j > 126:  # ascii printable characters
                                        text = False
                                        break
                                if text:
                                    updated_values.append(
                                        f"'{value.decode('latin_1')}'"
                                    )
                                # If the value contains non-printable characters, use hex instead
                                else:
                                    hex_value = value.hex().upper()
                                    updated_values.append(f"0x{hex_value}")
                            # Otherwise print binary in 0x hex form
                            else:
                                hex_value = value.hex().upper()
                                updated_values.append(f"0x{hex_value}")
                        # String
                        elif isinstance(value, str):
                            escaped_value = value.replace("'", "\\'")
                            updated_values.append(f"'{escaped_value}'")
                        # Number
                        else:
                            # mob_droplist and pet_skills use variables for certain fields
                            if (
                                table_name == "mob_droplist"
                                and i == 5
                                and str(value) in sql_variables
                            ):
                                updated_values.append(sql_variables[str(value)])
                            elif table_name == "pet_skills" and i == 9:
                                var_list = []
                                for var in sql_variables.keys():
                                    if value & int(var):
                                        var_list.append(sql_variables[var])
                                updated_values.append(" | ".join(var_list))
                            else:
                                # Get float formatting from the cursor description.
                                # https://github.com/mariadb-corporation/mariadb-connector-python/blob/67d3062ad597cca8d5419b2af2ad8b62528204e5/mariadb/mariadbcursor.c#L777-L787
                                if (
                                    Database.cursor.description[i][1]
                                    == FIELD_TYPE.FLOAT
                                    and Database.cursor.description[i][5] > 0
                                ):
                                    updated_values.append(
                                        f"{value:.{Database.cursor.description[i][5]}f}"
                                    )
                                else:
                                    updated_values.append(str(value))
                    values = ",".join(updated_values)
                    # Replace the values in the current line with the values pulled from the database
                    updated_line = line[: insert_start.end()] + f"{values});"
                    # Append any comments, preserving whitespace
                    if "--" in line:
                        insert_end = line.index(");") + 2
                        before_comment = line[insert_end:].split("--")[0]
                        updated_line = f"{updated_line}{before_comment}{line[insert_end + len(before_comment):]}"
                    else:
                        updated_line = f"{updated_line}\n"
                    updated_lines.append(updated_line)
                    row_index += 1
                # Otherwise just save the line as-is
                else:
                    updated_lines.append(line)

            # Write the updated content back to the file
            with open(
                f"{os.path.join(Common.root_path, 'sql', f'{table_name}.sql')}",
                "w",
                encoding="utf-8",
            ) as file:
                file.writelines(updated_lines)

        except Exception as e:
            print(f"Database error: {e}")

    @classmethod
    def fetch_files(cls) -> list[str]:
        """Get a list of project .sql files

        Returns a list of file names. If `express` is True, will
        compare current sql and migrations directories to previous
        revision. If any changes are found, `express_enabled` is set to
        True and only those changed files are returned.
        Also scans for sql modules.
        """
        import_files = []
        if Common.configs["db_ver"] and Common.version:
            try:
                sql_diffs = Common.repo.commit(Common.configs["db_ver"]).diff(
                    Common.version,
                    paths=os.path.join(Common.root_path, "sql"),
                )
                if len(sql_diffs) > 0:
                    for diff in sql_diffs:
                        if os.path.exists(os.path.join(Common.root_path, diff.b_path)):
                            import_files.append(
                                os.path.join(Common.root_path, diff.b_path)
                            )
            except Exception as e:
                Common.print_red(
                    "Error checking diffs.\nCheck that db_ver hash is valid."
                )
                print(e)
        else:
            for _, _, filenames in os.walk(os.path.join(Common.root_path, "sql")):
                for filename in sorted(filenames):
                    if (
                        filename.endswith(".sql")
                        and filename[:-4] not in Database.protected_tables
                    ):
                        import_files.append(
                            os.path.join(Common.root_path, "sql", filename)
                        )
                break
        import_files.sort()
        try:
            import_files.append(
                import_files.pop(
                    import_files.index(
                        os.path.join(Common.root_path, "sql", "triggers.sql")
                    )
                )
            )
        except Exception:
            pass
        with open(os.path.join(Common.root_path, "modules", "init.txt"), "r") as file:
            for line in file.readlines():
                if (
                    not line.startswith("#")
                    and line.strip()
                    and not line in ["\n", "\r\n"]
                ):
                    line = os.path.join(Common.root_path, "modules", line.strip())
                    if pathlib.Path(line).is_dir():
                        for filename in sorted(pathlib.Path(line).glob("**/*.sql")):
                            import_files.append(str(filename))
                    else:
                        if line.endswith(".sql"):
                            import_files.append(str(line))
        return import_files

    @classmethod
    def populate_migrations(cls) -> list[types.ModuleType]:
        """Scan for and import migrations."""
        migrations = []
        for file in sorted(
            os.scandir(os.path.join(Common.root_path, "tools", "migrations")),
            key=lambda e: e.name,
        ):
            if file.name.endswith(".py") and file.name != "utils.py":
                name = file.name.replace(".py", "")
                module = importlib.import_module("migrations." + name)
                migrations.append(module)
        return migrations

    @classmethod
    def check_missing(cls, table_list: list[str]) -> list[str]:
        """Check for any tables in `table_list` that are missing from the database.

        Parameters
        ----------
        table_list : list[str]
            A list of tables to check for.

        Returns
        -------
        list[str]
            A list of tables in `table_list` that are not in the database
        """
        tables = []
        for table in table_list:
            tables.append(f"'{table}'")
        Database.cursor.execute(
            f"SELECT TABLE_NAME FROM `information_schema`.`tables` WHERE `TABLE_SCHEMA` = '{Common.settings['network']['SQL_DATABASE']}' AND `TABLE_NAME` IN ({', '.join(tables)})"
        )
        tables = Database.cursor.fetchall()
        missing_tables = []
        for value in tables:
            missing_tables.append(value)

        return missing_tables

    @classmethod
    def fetch_errors(cls, result: subprocess.CompletedProcess) -> list[str]:
        """Redirect errors through this to hide annoying password warning."""
        errors = []
        for line in result.stderr.splitlines():
            if (
                "Using a password on the command line interface can be insecure"
                not in line
            ):
                errors.append(line)
        return errors

    @classmethod
    def check_mysql_bin(cls) -> bool:
        """Check access to MySQL binary files (mysql, mysqladmin, mysqldump)"""
        try:
            _ = subprocess.run(
                [f'{os.path.join(Common.configs["mysql_bin"], "mysql")}', "--version"],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return True
        except FileNotFoundError:
            mysql_env = shutil.which("mysql")
            if mysql_env:
                Common.configs["mysql_bin"] = os.path.normpath(
                    os.path.realpath(os.path.dirname(mysql_env))
                )
                Common.write_configs()
                return True
            else:
                Common.print_red(
                    "Add MySQL to your system path or set mysql_bin in config.yaml."
                )
        except subprocess.CalledProcessError:
            Common.print_red("Something went wrong while checking for MySQL directory.")
        return False


def main():
    import argparse

    if not Database.check_mysql_bin():
        Common.close(-1)

    try:
        ###############
        # Define Args #
        ###############
        parser = argparse.ArgumentParser(description="Manage the database.")
        subparsers = parser.add_subparsers(help="commands", dest="command")
        # Update command parser
        update_parser = subparsers.add_parser(
            "update", aliases=["u"], help="Check and perform database update"
        )
        update_parser.add_argument(
            "-a",
            "--all",
            action="store_true",
            help="Force update all tables",
        )
        # Import command parser
        import_parser = subparsers.add_parser(
            "import", aliases=["i"], help="Import a .sql file to the database"
        )
        import_parser.add_argument("import_file", action="store", help="File to import")
        import_parser.add_argument(
            "--yes",
            default=False,
            action="store_true",
            help="Confirm import (required)",
        )
        # Dump command parser
        export_parser = subparsers.add_parser(
            "export",
            aliases=["e"],
            help="Export the values from a table in the database to its .sql file",
        )
        export_parser.add_argument(
            "table_name",
            nargs="?",
            default=None,
            action="store",
            help="Table to export",
        )
        export_parser.add_argument(
            "-a",
            "--all",
            action="store_true",
            help="Export all tables",
        )
        # Setup command parser
        setup_parser = subparsers.add_parser("setup", help="Create a fresh database")
        setup_parser.add_argument(
            "database_name",
            action="store",
            help="Database to create (must match network.lua)",
        )
        setup_parser.add_argument(
            "--yes", default=False, action="store_true", help="Confirm setup (required)"
        )
        # Backup option
        parser.add_argument(
            "-b",
            "--backup",
            action="store_true",
            help="Backup full database, will be performed before other commands",
        )
        parser.add_argument(
            "-l",
            "--lite",
            action="store_true",
            help="Backup only protected tables, will be performed before other commands",
        )
        ###################
        # End Args Define #
        ###################

        args = parser.parse_args()
        status = 0

        # Backup
        if args.backup or args.lite:
            print("Creating backup...")
            out_dir = os.path.join(Common.root_path, "sql", "backups")
            result = Database.backup(
                out_dir=out_dir,
                tables=Database.protected_tables if args.lite else None,
            )
            errors = Database.fetch_errors(result)
            if errors:
                print(
                    f"{Common.LINE_UP}Errors occurred while creating backup.{Common.ERASE_TO_END_OF_LINE}"
                )
                for error in errors:
                    Common.print_red(error)
            else:
                Common.print_green(
                    f"{Common.LINE_UP}Backup created at {out_dir}{Common.ERASE_TO_END_OF_LINE}"
                )
            status = result.returncode
        if status == 0 and args.command:
            # Setup
            if args.command == "setup":
                if not args.yes:
                    Common.print_red(
                        'This command will drop the database if it exists. You must supply the "--yes" flag.'
                    )
                    status = -1
                elif args.database_name != Common.settings["network"]["SQL_DATABASE"]:
                    Common.print_red(
                        f'"{args.database_name}" does not match database "{Common.settings["network"]["SQL_DATABASE"]}" in network.lua.'
                    )
                    status = -1
                else:
                    status = status if Database.create() else -1
            else:
                Database.connection.database = Common.settings["network"][
                    "SQL_DATABASE"
                ]
                # Update
                if args.command == "u" or args.command == "update":
                    migrations = Database.populate_migrations()

                    Database.update()
                # Import
                elif args.command == "i" or args.command == "import":
                    if not args.yes:
                        Common.print_red(
                            'You must supply the "--yes" flag when manually importing files.'
                        )
                        status = -1
                    elif not os.path.exists(args.import_file):
                        Common.print_red(
                            f"Import file does not exist or is an incomplete path. ({args.import_file})"
                        )
                        status = -1
                    else:
                        print(f"Importing {args.import_file}...")
                        errors = Database.fetch_errors(
                            Database.import_file(args.import_file)
                        )
                        print(Common.LINE_UP, Common.ERASE_TO_END_OF_LINE, end="\r")
                        if errors:
                            print(
                                f"Errors occurred while importing {args.import_file}."
                            )
                            for error in errors:
                                Common.print_red(error)
                            status = -1
                        else:
                            Common.print_green(f"Imported {args.import_file}")
                # Export
                elif args.command == "e" or args.command == "export":
                    if not args.table_name and not args.all:
                        Common.print_red(
                            'You must supply a table to export, or supply the "--all" flag.'
                        )
                        status = -1
                    else:
                        if args.all:
                            export_tables = []
                            for _, _, filenames in os.walk(
                                os.path.join(Common.root_path, "sql")
                            ):
                                for filename in sorted(filenames):
                                    if filename.endswith(".sql"):
                                        export_tables.append(filename[:-4])
                                break
                            export_tables.remove("triggers")
                            for table in export_tables:
                                if table not in Database.protected_tables:
                                    print(f"Exporting {table}...")
                                    Database.export_table(table)
                                    print(
                                        Common.LINE_UP,
                                        Common.ERASE_TO_END_OF_LINE,
                                        end="\r",
                                    )
                        else:
                            print(f"Exporting {args.table_name}...")
                            Database.export_table(args.table_name)
                            print(Common.LINE_UP, Common.ERASE_TO_END_OF_LINE, end="\r")
                        Common.print_green("Export complete.")
        Common.close(status)

    except KeyboardInterrupt:
        Common.close(status)


def test():
    """
    create
    backup
    query(DROP)
    import(backup)
    export

    update
    repair
    """
    pass


if __name__ == "__main__":
    main()
