import subprocess as sp
from typing import Tuple

from inno_utils.loggers import log


def execute_local_shell_command(
    command: str,
    cwd: str = None,
    echo_stdout: bool = True,
    raise_on_stderr: bool = True,
    raise_on_errcode: bool = True,
) -> Tuple[int, str, str]:
    """
    Execute the given command on the host machine.

    Parameters
    ----------
    command:
        Command to execute on the local machine

    cwd:
        Working directory to execute the command in

    echo_stdout:
        Stream the standard out of the command as it's produced

    raise_on_stderr:
        Raise an exception if there is any output in stderr

    raise_on_errcode:
        Raise an exception if the exit code of the command is non-zero

    Returns
    -------
    exit_code:
        Exit code of the command

    out:
        Output of stdout

    err:
        Output of stderr
    """
    log.info(f"Start executing local shell command: '{command}'")
    pipe = sp.Popen(
        command,
        cwd=cwd,
        shell=True,
        stdout=sp.PIPE,
        stderr=sp.STDOUT,  # https://stackoverflow.com/a/4418193 to solve [SPRK-411]
        universal_newlines=True,
    )
    # log real-time output
    out = ""
    while True:
        out_line = pipe.stdout.readline()
        if out_line == "" and pipe.poll() is not None:
            break
        if out_line:
            out += out_line
            if echo_stdout:
                log.info(out_line.rstrip())
    # get final output and error of the process
    _, err = pipe.communicate()
    exit_code = pipe.returncode
    pipe.kill()
    log.info(f"Done executing local shell command: '{command}'")

    if err:
        log.error(f"Stderr: {err}")
        if raise_on_stderr:
            raise Exception("Error executing command")

    if exit_code != 0:
        log.error(f"Exit code: {exit_code} returned non-zero")
        if raise_on_errcode:
            raise Exception(f"Command exited with status {exit_code}: {err} {out}")

    return exit_code, out, err
