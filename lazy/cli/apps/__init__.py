

from .base import baseCli
from .serialize_app import *

baseCli.add_typer(encodeCli, name='encode')
baseCli.add_typer(decodeCli, name='decode')
baseCli.add_typer(keysCli, name='keys')
baseCli.add_typer(readCli, name='read')
baseCli.add_typer(writeCli, name='write')


if __name__ == '__main__':
    baseCli()