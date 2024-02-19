import inspect
import os
import sys

import yaml
import git
from git.exc import InvalidGitRepositoryError


class Common:
    # Default configs
    configs: dict = {"db_ver": None, "mysql_bin": ""}

    tools_path = os.path.abspath(
        os.path.dirname(os.path.dirname(inspect.getfile(inspect.currentframe())))
    )
    root_path = os.path.abspath(os.path.dirname(tools_path))

    LINE_UP = "\033[1A"
    ERASE_TO_END_OF_LINE = "\033[K"
    color = {
        "red": "\033[31m",
        "green": "\033[32m",
        "reset": "\033[39m",
    }

    try:
        repo = git.Repo(root_path)
        version = repo.git.rev_parse(repo.head.object.hexsha, short=8)
    except InvalidGitRepositoryError:
        repo = None
        version = ""
    settings = {}

    def __init__(self) -> None:
        Common.populate_settings()

    @classmethod
    def print_red(cls, str) -> None:
        print(Common.color["red"] + str + Common.color["reset"])

    @classmethod
    def print_green(cls, str) -> None:
        print(Common.color["green"] + str + Common.color["reset"])

    @classmethod
    def close(cls, status: int = 0, preflight: bool = False) -> None:
        """Exits the program.

        Parameters
        ----------
        status : int, optional
            Optional exit code
        preflight : bool, default=False
            Pause for input if double clicked on Windows before exit
        """
        try:
            if preflight:
                # So the user can read the error
                if os.name == "nt" and "PROMPT" not in os.environ:
                    input("Press ENTER to continue...")
            sys.exit(status)
        except SystemExit:
            os._exit(status)

    @classmethod
    def fetch_configs(cls) -> None:
        try:
            if os.path.exists(os.path.join(Common.tools_path, "config.yaml")):
                with open(os.path.join(Common.tools_path, "config.yaml")) as file:
                    Common.configs = yaml.safe_load(file)
            else:
                Common.write_configs()
        except Exception as e:
            Common.print_red(e)

    @classmethod
    def write_configs(cls) -> None:
        with open(os.path.join(Common.tools_path, "config.yaml"), "w") as file:
            yaml.dump(Common.configs, file)

    @classmethod
    def populate_settings(cls) -> None:
        for path_to_settings in [
            os.path.join(Common.root_path, "settings", "default"),
            os.path.join(Common.root_path, "settings"),
        ]:
            for filename in os.listdir(path_to_settings):
                filename = os.path.join(path_to_settings, filename)
                if os.path.exists(os.path.join(filename)) and os.path.isfile(filename):
                    try:
                        with open(filename) as f:
                            filename_key = filename[:-4].split(os.sep)[-1]
                            # Get or default, so we update any existing dict
                            # instead of wiping it out
                            current_settings = Common.settings.get(filename_key, {})
                            for line in f.readlines():
                                if not line:
                                    break
                                if "=" in line:
                                    # remove newline
                                    line = line.replace("\n", "")
                                    # NOTE: Do not use split or rsplit in herewithout a counter,
                                    #     : to make sure you leave the contents of val alone!
                                    parts = line.split("=", 1)
                                    key = parts[0].strip()
                                    val = parts[1].strip()
                                    # ignore commented out entries
                                    if key.startswith("--"):
                                        continue
                                    # strip off comments
                                    val = val.rsplit("--")[0].strip()
                                    # pop off leading quote
                                    if val.startswith('"'):
                                        val = val[1:]
                                    # pop off trailing comma
                                    if val.endswith(","):
                                        val = val[:-1]
                                    # pop off trailing quote
                                    if val.endswith('"'):
                                        val = val[:-1]
                                    current_settings[key] = val
                                Common.settings[filename_key] = current_settings
                    except Exception as e:
                        print("Error populating settings.")

    # Initial config population
    try:
        if os.path.exists(os.path.join(tools_path, "config.yaml")):
            with open(os.path.join(tools_path, "config.yaml")) as file:
                configs = yaml.safe_load(file)
        else:
            with open(os.path.join(tools_path, "config.yaml"), "w") as file:
                yaml.dump(configs, file)
    except Exception as e:
        print_red(e)


if __name__ == "__main__":
    import inspect

    methods = []
    print("\nCommon:")
    print("- Attributes:")
    for attribute in inspect.getmembers(Common):
        if not attribute[0].startswith("_"):
            if not inspect.ismethod(attribute[1]):
                print(" - ", attribute)
            else:
                methods.append(attribute)
    if methods:
        print("- Methods:")
        for method in methods:
            print(" - ", method)
