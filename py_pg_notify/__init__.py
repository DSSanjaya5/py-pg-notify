from .listener import Listener

def get_version():
    try:
        with open("../pyproject.toml", "r") as f:
            lines = f.readlines()
            for line in lines:
                # Search for the version line
                if line.strip().startswith("version = "):
                    # Extract version by removing 'version = ' and quotes
                    version = line.split("=")[1].strip().strip('"')
                    return version
            raise RuntimeError("Version not found in pyproject.toml")
    except Exception as e:
        raise RuntimeError(f"Error reading version from pyproject.toml: {e}")

__version__ = get_version()
