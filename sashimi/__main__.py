import os
import json
import datetime
import dotenv
import sys
import time
import requests
import typer
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
                  rich_markup_mode="markdown"
                  )
err_console = Console(stderr=True)

from . import SashimiClient

args = None
sashimi: SashimiClient = None

dsarg = Annotated[str, typer.Argument(
        metavar='DATASET_NAME',
        help='dataset name'
        )]

#@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})

panel_write="'Write' commands (rarely needed, use upload/import instead)"
panel_main="Main commands, each has its own help, e.g. sashimi upload --help"

@app.command(rich_help_panel=panel_main)
def rm(ds: dsarg):
    """ Remove dataset from Sashimi """
    try:
        result = sashimi.rm(ds_name=ds)
    except requests.RequestException as e:        
        err_console.print(f'{e!r}')
        err_console.print(f'{e.response.text!r}')
        sys.exit(1)

    print(result)

@app.command(rich_help_panel=panel_write)
def delete(ds: dsarg,       
    expr: Annotated[str, typer.Option('--expr', '-e',
        help='Pythonic expression, instead of filter. E.g.: brand="Apple" and price<=100',        
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

@app.command(rich_help_panel=panel_write)
def update(ds: dsarg,
    field: Annotated[str, typer.Argument(help='field to update, e.g. "price" or "onstock"')],
    expr: Annotated[str, typer.Argument(
        help='Pythonic expression for selected records. E.g.: 200 or price+20 or False',
        )],
    where: Annotated[str, typer.Argument(
        help='Pythonic expression, like SQL WHERE. E.g.: brand="Apple" and price<=100',        
        )],
    ):
    """ Update records in Sashimi dataset 
    Examples:
    update products onstock False 'id=123'
    update products price price+20 'id=123'
    """
    try:
        result = sashimi.update(ds_name=ds, field=field, where_expr=where, update_expr=expr)
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
    filter: Annotated[list[str], typer.Argument(help='list of filters like: category="laptop" price__lt=100')] = None,
    expr: Annotated[str, typer.Option('--expr', '-e',
        help='Pythonic expression, instead of filter. E.g.: \'brand="Apple" and price<=100\'',
        )] = None,
    limit: Annotated[int, typer.Option(
        help='limit results to N ',
        )] = None,
    sort: Annotated[str, typer.Option(
        help='sort by this field',
        )] = None,
    reverse: Annotated[bool, typer.Option(
        '--reverse / ', 
        help='reverse sort order',
        )] = False,
    fields: Annotated[list[str], typer.Option('-f', '--fields',
        help='retrieve only these fields (repeat)',        
        )] = None,
    aggregate: Annotated[list[str], typer.Option('-a', '--aggregate',
        help='aggregate functions (min/max/sum/avg/distinct), e.g. min:price, distinct:brand'
    )] = None,
    discard: Annotated[bool, typer.Option(
        '--discard / ', '-d / ',
        help='discard results',
        )] = False,
    result: Annotated[bool, typer.Option(
        '-r / ', '--result / ',
        help='print results only',
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



@app.command(name='import', rich_help_panel=panel_main)
def dbimport(
    db: Annotated[str, typer.Argument(
        metavar='database',
        help='db url, example: mysql://scott:tiger@127.0.0.1/contacts')],
    sql: Annotated[str, typer.Argument(
        metavar='SELECT',
        help='SELECT * FROM mytable')],
    ds_name: dsarg,
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
            elif v.__class__.__name__ == 'Decimal':
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
        result = sashimi.put(ds_name, dataset=dataset)
    except (ValueError, requests.RequestException) as e:
        print(e)
        print(e.response.text)
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
    try:
        result = sashimi.put(ds_name, dataset=dataset)
    except (ValueError, requests.RequestException) as e:
        print(e)
        print(e.response.text)
        sys.exit(1)

    # r = exact.query(ds_name, expr='True', limit=2)
    # print(len(r['result']))
    print(result)


def main():
    global exact

    dotenv_file = os.getenv('SASHIMI_DOTENV', '.env')
    dotenv.load_dotenv(dotenv_file)
    # get_args()

    # exact = SashimiClient(base_url=args.server, project=args.project, token=args.token)
    # typer.rich_utils.Panel = Panel.fit
    app()

if __name__ == '__main__':
    main()