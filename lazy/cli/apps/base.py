from typer import Typer, Option, Argument, echo, colors, style
from typer.testing import CliRunner
from lazy.types import *
from lazy.utils import get_logger

logger = get_logger('lazycli')


"""
name: Optional[str] = Default(None),
cls: Optional[Type[click.Command]] = Default(None),
invoke_without_command: bool = Default(False),
no_args_is_help: bool = Default(False),
subcommand_metavar: Optional[str] = Default(None),
chain: bool = Default(False),
result_callback: Optional[Callable[..., Any]] = Default(None),
# Command
context_settings: Optional[Dict[Any, Any]] = Default(None),
callback: Optional[Callable[..., Any]] = Default(None),
help: Optional[str] = Default(None),
epilog: Optional[str] = Default(None),
short_help: Optional[str] = Default(None),
options_metavar: str = Default("[OPTIONS]"),
add_help_option: bool = Default(True),
hidden: bool = Default(False),
deprecated: bool = Default(False),
add_completion: bool = True,
"""

def createCli(name: str, invoke_without_command: bool = False, chain: bool = False, result_callback: Optional[Callable[..., Any]] = None, **kwargs):
    return Typer(name=name, invoke_without_command=invoke_without_command, chain=chain, result_callback=result_callback, **kwargs)

baseCli = Typer()

__all__ = [
    'List',
    'Optional',
    'Typer',
    'Option',
    'Callable',
    'Argument',
    'echo',
    'style',
    'colors',
    'createCli',
    'CliRunner',
    'logger',
]