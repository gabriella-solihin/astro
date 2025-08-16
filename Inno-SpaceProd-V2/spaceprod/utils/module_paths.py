"""
This object provide access to various paths in the repo
"""
import os

from inno_utils.loggers import log


class RepoPaths:
    @property
    def root(self) -> str:
        import spaceprod

        path_space = spaceprod.__path__[0]
        return os.path.dirname(path_space)

    @property
    def setup_py(self) -> str:
        return os.path.join(self.root, "setup.py")

    @property
    def temp_folder(self) -> str:
        """
        an in-repo temp folder to store temporary output locally
        it is added to .gitignore
        """
        path = os.path.join(self.root, "temp")

        # TODO maybe doing this here goes beyond the scope of what this
        #  class is designed for, but generally for "temp folder" purposes
        #  it might be ok
        if not os.path.isdir(path):
            log.info(f"Creating temp folder: {path}")
            os.makedirs(path)

        return path


repo_paths = RepoPaths()
