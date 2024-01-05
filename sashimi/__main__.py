import os
import json
import yaml
import datetime
import dotenv
import sys
import time
import requests
import typer
import click

from pathlib import Path
from typing_extensions import Annotated
from typing import Optional

from rich import print
from rich.console import Console
from rich.panel import Panel
from rich.progress import track

from sqlalchemy import create_engine
from sqlalchemy.engine.row import RowMapping
import sqlalchemy as sa

app = typer.Typer(pretty_exceptions_show_locals=False, 
    # rich_markup_mode="rich"
    no_args_is_help=True,                
    rich_markup_mode="markdown"
    )
err_console = Console(stderr=True)

from . import SashimiClient

args = None
sashimi: SashimiClient = None

dsarg = Annotated[str, typer.Argument(
        metavar='DATASET',
        help='dataset name',
        show_default=False
        )]

#@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})

panel_write="'Write' commands (rarely needed, use upload/import instead)"
panel_main="Main commands, each has its own help, e.g. sashimi upload --help"
panel_config="Dataset and project configs"

@app.command(rich_help_panel=panel_main,
             help='Remove dataset from Sashimi',
            epilog="""~~~shell\n
sashimi rm products\n
~~~
"""
             )
def rm(ds: dsarg):
    """ Remove dataset from Sashimi """
    try:
        result = sashimi.rm(ds_name=ds)
    except requests.RequestException as e:        
        err_console.print(f'{e!r}')
        err_console.print(f'{e.response.text!r}')
        sys.exit(1)

    print(result)

@app.command(rich_help_panel=panel_write,
             help='Delete records from Sashimi dataset',
            epilog="""~~~shell\n
sashimi delete products 'id==1'\n
~~~
"""
)
def delete(ds: dsarg,       
    expr: Annotated[str, typer.Argument(
        help='Pythonic expression',
        )],
    ):
    """ Delete records from Sashimi dataset """
    try:
        result = sashimi.delete(ds_name=ds, expr=expr)
    except requests.RequestException as e:        
        err_console.print(f'{e!r}')
        err_console.print(f'{e.response.text!r}')
        sys.exit(1)

    print(result)

@app.command(rich_help_panel=panel_write,
            help='Update records in Sashimi dataset',
            epilog="""~~~shell\n
sashimi update products 'id==1' '{"price": 125, "stock": 200}'\n
~~~
"""
             )
def update(ds: dsarg,
    where: Annotated[str, typer.Argument(
        help='Pythonic filter expression (like SQL WHERE). E.g.: \'brand="Apple" and price<=100\'',
        show_default=False,
        )],
    data: Annotated[str, typer.Argument(
        help='New value (json) for this field in selected records. E.g.: {"price": 200, "title": "New title"}',
        show_default=False,
        )],
    ):
    """ Update records in Sashimi dataset 
    ~~~shell
    # update record, set new values for two fields
    sashimi update products 'id==1' '{"price": 125, "stock": 0}'
    ~~~
    """
    try:
        result = sashimi.update(ds_name=ds, expr=where, data=data)
    except requests.RequestException as e:        
        err_console.print(f'{e!r}')
        err_console.print(f'{e.response.text!r}')
        sys.exit(1)

    print(result)


@app.command(help="Insert JSON record into Sashimi dataset", rich_help_panel=panel_write)
def insert(ds: dsarg,
    datastr: Annotated[str, typer.Argument(help='field to update, e.g. "price" or "onstock"')]
    ):

    try:
        data = json.loads(datastr)
    except json.JSONDecodeError as e:
        err_console.print(f'JSON error: {e}')
        sys.exit(1)    

    try:
        result = sashimi.insert(ds_name=ds, data=data)
    except requests.RequestException as e:        
        err_console.print(f'{e!r}')
        err_console.print(f'{e.response.text!r}')
        sys.exit(1)

    print(result)




@app.command(rich_help_panel=panel_main,
             help="Query Sashimi dataset",
             epilog="""~~~shell\n
    # get by id:\n
    sashimi query products id=42\n\n\n
    # simple search:\n
    sashimi query products 'price<1000' 'category="smartphones"' 'brand=["Apple", "Huawei"]'\n\n\n
    # same search but with pythonic expression:\n
    sashimi query products --expr 'price < 1000 and category=="smartphones" and brand in ["Apple", "Huawei"]'\n\n\n
    # aggregation:\n
    sashimi query products --discard 'category="smartphones"' -a min:price -a max:price -a distinct:brand\n
    ~~~"""
)

def query(
    ds: dsarg,
    filter: Annotated[list[str], typer.Argument(help="""list of filters like: 'category="laptop"' 'price<1000'""", show_default=False)] = None,
    expr: Annotated[str, typer.Option('--expr', '-e',
        help='Pythonic expression (instead of filter) E.g.: \'brand="Apple" and price<=100\'',
        show_default=False,
        )] = None,
    limit: Annotated[int, typer.Option(
        help='limit results to N ',
        show_default=False
        )] = None,
    sort: Annotated[str, typer.Option(
        help='sort by this field',
        show_default=False,
        )] = None,
    reverse: Annotated[bool, typer.Option(
        '--reverse / ', 
        help='reverse sort order',
        )] = False,
    fields: Annotated[list[str], typer.Option('-f', '--fields',
        help='retrieve only these fields (repeat)',
        show_default=False,
        )] = None,
    aggregate: Annotated[list[str], typer.Option('-a', '--aggregate',
        help='aggregate functions (min/max/sum/avg/distinct), e.g. min:price, distinct:brand',
        show_default=False
    )] = None,
    discard: Annotated[bool, typer.Option(
        '--discard / ', '-d / ',
        help='discard results',
        show_default=False,
        )] = False,
    result: Annotated[bool, typer.Option(
        '-r / ', '--result / ',
        help='print results only',
        show_default=False,
        )] = False,

    ):
    """
    Query sashimi dataset. 
    """

    def filter_convert(flist: list[str]):        
        sepsuffix = {
            '>=': '__ge',
            '<=': '__le',
            '=': '',
            '>': '__gt',
            '<': '__lt'
        }

        for f in flist:
            for sep in sepsuffix:
                if sep in f:
                    k, v = f.split(sep, maxsplit=1)                    
                    try:
                        vv = json.loads(v)
                    except json.JSONDecodeError as e:
                        err_console.print(f'Invalid filter: {f!r}')
                        sys.exit(1)
                    kk = k + sepsuffix[sep]
                    yield kk, vv
                    break
            else:                
                raise ValueError(f'Not found any separator in {f!r}')


    """ I can do dict/list comprehensions, I can do ugly code """
    fdict={ field: value for field, value in filter_convert(filter) }

    try:
        r = sashimi.query(ds, filter=fdict, 
                           expr=expr,
                           limit=limit, 
                           sort=sort, reverse=reverse,
                           fields=fields,
                           discard=discard,
                           aggregate=aggregate)
    
    except requests.RequestException as e:
        err_console.print(f'{e!r}')
        err_console.print(f'{e.response.text!r}')
        sys.exit(1)



    if result:
        print(r['result'])
    else:
        print(r)


@app.command(rich_help_panel=panel_main,
             help="Run named query",
             epilog="""~~~shell\n
    # run 'sale' query (e.g. products with some flag is True):\n
    sashimi query products sale\n\n\n
    ~~~"""
)
def named(
        ds:dsarg,
        name: Annotated[str, typer.Argument(
            help='run pre-configured named search (if configured in dataset config)',
        )] = 'index',
        result: Annotated[bool, typer.Option(
            '-r / ', '--result / ',
            help='print results only',
            show_default=False,
            )] = False,
):
    r = sashimi.named_query(ds_name=ds, name=name)
    if result:
        print(r['result'])
    else:
        print(r)



@app.callback(
        context_settings={"help_option_names": ["-h", "--help"]})

def callback(ctx: typer.Context,
    project: Annotated[str, typer.Option(envvar='SASHIMI_PROJECT', rich_help_panel='Config (.env file)', help='URL of Sashimi project, e.g.: http://localhost:8000/ds/sandbox')],
    token: Annotated[str, typer.Option(envvar='SASHIMI_TOKEN', rich_help_panel='Config (.env file)')]
    ):
    """
    Client for Sashimi headless CMS
    """
    global sashimi

    # typer.echo(f"About to execute command: {ctx.invoked_subcommand}")

    sashimi = SashimiClient(project_url=project, token=token)


@app.command(rich_help_panel=panel_main)
def info():
    """ Info about project """
    # check_arguments("info")
    result = sashimi.info()
    print(json.dumps(result, sort_keys=True, indent=4))


@app.command(name='import', rich_help_panel=panel_main,
             help='Upload database content to Sashimi project',
             epilog="""
            Do not forget to install proper python packages for SQLAlchemy if you want to import from mysql/postgresql/... databases. Sqlite3 support is built-in. 

             ~~~shell\n
             # import all books from mysql/mariadb, no password\n
             sashimi import mysql://scott@localhost/libro 'SELECT * FROM libro' libro\n
             \n
             # sqlite db libro.db in current directory\n
             sashimi import sqlite:///libro.db 'SELECT * FROM libro' libro\n
             \n
             # import all cheap books from postgresql\n
             sashimi import postgresql://scott:tiger@localhost/libro 'SELECT * FROM libro WHERE price<20' libro\n
             ~~~
             """)
def dbimport(
    db: Annotated[str, typer.Argument(
        metavar='database',
        help='db url, example: mysql://scott:tiger@127.0.0.1/contacts',
        show_default=False,
        )],
    sql: Annotated[str, typer.Argument(
        metavar='SELECT',
        help='SELECT statement, any JOINs allowed, e.g. SELECT * FROM mytable',
        show_default=False)],
    ds_name: dsarg,
    secret: Annotated[Optional[str], typer.Option(
        '-s', '--secret',
        metavar='SECRET',
        help='secret for ds (only for sandbox projects)',
        show_default=False,
        )] = None
    ):

    """
    Upload database content to Sashimi project.
    """
    import_start = time.time()

    datetime_fmt = "%d/%m/%Y %H:%M:%S"

    def make_record(r: RowMapping):
        outdict = dict(r)
        for k, v in outdict.items():
            if isinstance(v, (int, str, type(None))):
                pass
            elif isinstance(v, datetime.datetime):
                outdict[k] = v.strftime('%d/%m/%Y %H:%M:%S')
            elif isinstance(v, datetime.date):
                outdict[k] = v.strftime('%d/%m/%Y')
            elif v.__class__.__name__ in ['float', 'Decimal']:
                outdict[k] = float(v)
            else:
                print(v)
                print(type(v))
                print(v.__class__.__name__)
                raise ValueError(f'Do not know how to process type {type(v)} for field {k}, value: {v!r}')

        return outdict


    assert(sql is not None)
    engine = create_engine(db)
    with engine.begin() as conn:
        qry = sa.text(sql)
        resultset = conn.execute(qry)

        # dataset = [ dict(x) for x in resultset.mappings().all() ]
        dataset = [ dict(make_record(x)) for x in track(resultset.mappings().all(), total=resultset.rowcount, description="Processing...") ]

    print(f"# Loaded from db dataset of {len(dataset)} records in {time.time() - import_start:.2f} seconds")
    try:
        result = sashimi.put(ds_name, dataset=dataset, secret=secret)
    except (ValueError, requests.RequestException) as e:
        print(e)
        print(e.response.text)
        sys.exit(1)

    print(result)


   
@app.command(rich_help_panel=panel_config, help='Get dataset config',
             name="",
             epilog="""~~~shell\n
sashimi getconfig mydataset\n
~~~""")
def getconfig(ds_name: dsarg,
    config: Annotated[Path, typer.Option('-w',
    metavar='CONFIG.yaml',
    help='Save dataset config to YAML file')] = None,
    ):
    config_data = sashimi.get_ds_config(ds_name)
    if config:
        with open(config, "w")as fh:
            fh.write(config_data)
    else:
        print(config_data)

@app.command(rich_help_panel=panel_config, help='Get project config',
             name="",
             epilog="""~~~shell\n
sashimi getpconfig\n
sashimi getpconfig -w myproject.yaml\n
~~~""")
def getpconfig(
    config: Annotated[Path, typer.Option('-w',
    metavar='CONFIG.yaml',
    help='Save project config to yaml config ')] = None,
    ):
    config_data = sashimi.get_project_config()
    if config:
        with open(config, "w")as fh:
            fh.write(config_data)
    else:
        print(config_data)



@app.command(rich_help_panel=panel_config, help='Set dataset config',
             epilog="""~~~shell\n
sashimi setconfig mydataset\n
~~~""")
def setconfig(ds_name: dsarg,
        config: Annotated[Path, typer.Argument(
        metavar='CONFIG.yaml',
        help='Upload yaml config for dataset')],
):        
    try:
        result = sashimi.set_ds_config(ds_name, path=config)
    except yaml.YAMLError as e:
        err_console.print(e)
        sys.exit(1)
    print(result)
    
@app.command(rich_help_panel=panel_config, help='Set project config',
             epilog="""~~~shell\n
sashimi setpconfig \n
~~~""")
def setpconfig(config: Annotated[Path, typer.Argument(
        metavar='CONFIG.yaml',
        help='Upload yaml config for project')],
):        
    try:
        result = sashimi.set_project_config(path=config)
    except yaml.YAMLError as e:
        err_console.print(e)
        sys.exit(1)
    print(result)


@app.command(rich_help_panel=panel_main, help='Upload JSON dataset to Sashimi project',
             epilog="""~~~shell\n
sashimi upload file.json mydataset\n
~~~""")
def upload(
    file: Annotated[typer.FileText, typer.Argument(
        metavar='FILE.json',
        help='JSON dataset to upload (list of dicts, or use --key)')],
    ds_name: dsarg,
    keypath: Annotated[Optional[list[str]], typer.Option(
        '--key',
        metavar='KEY',
        help='list of keys to dive-in to dataset in JSON',
        show_default=False,
        )] = None,
    secret: Annotated[Optional[str], typer.Option(
        '-s', '--secret',
        metavar='SECRET',
        help='secret for ds (only for sandbox projects)',
        show_default=False,
        )] = None

    ):

    """
    Upload JSON dataset to Sashimi project. Example: sashimi upload file.json mydataset
    """
   
    # read file

    dataset = json.load(file)

    if keypath:
        try:
            for key in keypath:
                    dataset = dataset[key]
        except KeyError as e:
            print("Not found key", e)
            sys.exit(1)

    if not isinstance(dataset, list):
        print(f"Incorrect dataset (type: {type(dataset)!r}, but need list)")
        sys.exit(1)
    
    if len(dataset):
        rec = dataset[0]
        if not isinstance(rec, dict):
            print(f"Incorrect dataset record (type: {type(dataset)!r}, but need dict)")
            sys.exit(1)

    print(f"# dataset: {len(dataset)} records")
    result = sashimi.put(ds_name, dataset=dataset, secret=secret)

    # r = exact.query(ds_name, expr='True', limit=2)
    # print(len(r['result']))
    print(result)


def main():
    global exact

    dotenv_file = os.getenv('SASHIMI_DOTENV', '.env')
    dotenv.load_dotenv(dotenv_file)
        
    # app()
    command = typer.main.get_command(app)

    project = None


    try:
        rc = command(standalone_mode=False)
    except requests.exceptions.ConnectionError as e:
        err_console.print(e)
        err_console.print(f'Maybe wrong SASHIMI_PROJECT or server is offline?')

    except requests.RequestException as e:
        err_console.print(e)
        if e.response.status_code == 500:
            pass
        else:
            err_console.print(f"{e.response.status_code} {e.response.text!r}")

    except click.ClickException as e:
        err_console.print(e)

    except Exception as e:
        err_console.print(type(e))
        err_console.print(f"Got unexpected exception: {e}")




if __name__ == '__main__':
    main()
