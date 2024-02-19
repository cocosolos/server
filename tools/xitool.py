import os

from utils.common import Common
from utils.database import Database


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


if __name__ == "__main__":
    main()
