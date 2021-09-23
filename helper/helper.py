from pathlib import Path
from fire import Fire
from jinja2 import Template
import yaml
import pyperclip


class Helper:
    def _load_defaults(self, file_path):
        """Load defaults from yaml file"""

        # Convert file_path to Path
        file_path = Path(file_path)

        # Open defaults.yaml file from same directory as file_path
        defaults_path = file_path.parent / "defaults.yaml"
        args = yaml.load(defaults_path.open("r"))

        # Open yaml file with same name of file_path
        try:
            yaml_path = file_path.parent / file_path.name.replace(".sql", ".yaml")
            local_args = yaml.load(yaml_path.open("r"))

            # Merge local_args with args
            args["parameters"].update(local_args)
        except FileNotFoundError:
            pass

        # Load inner args
        yaml_path = Path(__file__).parent / "inner_params.yaml"
        inner_args = yaml.load(yaml_path.open("r"))

        # Merge local_args with args
        args["parameters"].update(inner_args)

        print(args["parameters"])

        return args["parameters"]

    def render(self, file_path):
        """Render Jinja file with defaults arguments"""

        args = self._load_defaults(file_path)
        with open(file_path, "r") as f:
            query = Template(f.read()).render(**args)

        print(query)

        # copy data to clipboard
        pyperclip.copy(query)
        print("Query copied to clipboard")


if __name__ == "__main__":
    Fire(Helper)
