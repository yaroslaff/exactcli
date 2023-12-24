import os
import json
import argparse
import dotenv
import sys
import requests
import typer
from typing_extensions import Annotated
from typing import Optional
from rich import print
from rich.console import Console

app = typer.Typer(pretty_exceptions_show_locals=False)
err_console = Console(stderr=True)

from . import SashimiClient

args = None
sashimi: SashimiClient = None

dsarg = Annotated[str, typer.Argument(
        metavar='DATASET_NAME',
        help='dataset name'
        )]

#@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
@app.command()
def query(
    ds: dsarg,
    filter: Annotated[list[str], typer.Argument(help='list of filters like: category="laptop" price__lt=100')] = None,
    expr: Annotated[str, typer.Option('--expr', '-e',
        help='Pythonic expression, instead of filter. E.g.: brand="Apple" and price<=100',
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
    result: Annotated[bool, typer.Option(
        '-r / ', '--result / ',
        help='print results only',
        )] = False,

    ):
    """
    Query sashimi dataset. Example
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

    r = sashimi.query(ds, filter=fdict, 
                           expr=expr,
                           limit=limit, 
                           sort=sort, reverse=reverse,
                           fields=fields)
    
    if result:
        print(r['result'])
    else:
        print(r)

    return



    filter = {}
    if args.filter:
        for pair in args.filter:
            key, value = pair.split('=')
            filter[key] = json.loads(value)

    result = exact.query(args.ds, expr=args.expr, filter=filter, fields=args.fields, limit=args.limit, sort=args.sort, reverse=args.reverse)
    print(json.dumps(result, sort_keys=True, indent=4))


    sp = subparsers.add_parser('query', aliases=['q'], parents=[p], conflict_handler='resolve', help='query dataset')
    sp.add_argument('expr', metavar='EXPRESSION', help='Expression like \'True\' or \'id==1\' or \'Brand == "Apple" and price < 1000\'')
    sp.add_argument('--filter', nargs='*', metavar="KEY=VALUE", help='series key=value like: brand="Apple" price__lt=1000')











@app.callback(
        context_settings={"help_option_names": ["-h", "--help"]})
def callback(ctx: typer.Context,
    project: Annotated[str, typer.Option(envvar='SASHIMI_PROJECT', rich_help_panel='Config')],
    token: Annotated[str, typer.Option(envvar='SASHIMI_TOKEN', rich_help_panel='Config')]
    ):
    """
    Client for Sashimi headless CMS
    """
    global sashimi

    # typer.echo(f"About to execute command: {ctx.invoked_subcommand}")

    sashimi = SashimiClient(project_url=project, token=token)


@app.command()
def info():
    # check_arguments("info")
    result = sashimi.info()
    print(json.dumps(result, sort_keys=True, indent=4))

@app.command()
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


def make_parser():

    def_url = os.getenv('SASHIMI_URL')
    def_project = os.getenv('SASHIMI_PROJECT')
    def_dsname = os.getenv('SASHIMI_DSNAME')
    def_token = os.getenv('SASHIMI_TOKEN')

    p = argparse.ArgumentParser(description='Sashimi CLI client', formatter_class=argparse.RawTextHelpFormatter)
    p.epilog = 'use "SUBCOMMAND --help", e.g. "query --help", "upload -h"'


    # p.add_argument("-h", "--help", metavar='SECTION', nargs='?', const='all', help="show help for section or 'all'")
    # p.add_argument("--examples",  action='store_true', help="show simple examples")


    g = p.add_argument_group('Target specification (use .env)')
    g.add_argument('-s', '--server', metavar='SERVER', default=def_url, help=argparse.SUPPRESS) #help='Exact server url, e.g. https://exact.www-security.com/')
    g.add_argument('-p', '--project', metavar='PROJECTNAME', default=def_project, help=argparse.SUPPRESS)
    g.add_argument('--ds', '--dataset', metavar='DSNAME', default=def_dsname, help=argparse.SUPPRESS)
    g.add_argument('--token', metavar='TOKEN', default=def_token, help=argparse.SUPPRESS)


    """ subparsers """
    subparsers = p.add_subparsers(help='Subcommands', dest='subcommand')
    sp = subparsers.add_parser('info', parents=[p], conflict_handler='resolve', help='info about current project')


    sp = subparsers.add_parser('upload', aliases=['put', 'up'], parents=[p], conflict_handler='resolve', help='upload file to Sashimi')
    sp.add_argument('file')


    sp = subparsers.add_parser('query', aliases=['q'], parents=[p], conflict_handler='resolve', help='query dataset')
    sp.add_argument('expr', metavar='EXPRESSION', help='Expression like \'True\' or \'id==1\' or \'Brand == "Apple" and price < 1000\'')
    sp.add_argument('--filter', nargs='*', metavar="KEY=VALUE", help='series key=value like: brand="Apple" price__lt=1000')
    sp.add_argument('--limit', type=int, metavar='NUM', help='Limit')
    sp.add_argument('--sort', metavar='FIELD', help='Sort by this field')
    sp.add_argument('--reverse', default=False, action='store_true', help='reverse order for sort')
    sp.add_argument('--fields', metavar='FIELD', nargs='+', help='return only these fields')

    sp = subparsers.add_parser('examples', parents=[p], conflict_handler='resolve', help='show examples')

    return p


def examples():
    examples = """
Configuration stored in file .env:

SASHIMI_URL=https://exact.www-security.com/
SASHIMI_PROJECT=sandbox
SASHIMI_DATASET=products
SASHIMI_TOKEN=mytoken

You may override values from .env file with environment variables or options: --url --project --dataset (--ds) and --token
"""
    print(examples)


def get_args():
    global args
    
    me = "exactcli"

    parser = make_parser()

    args = parser.parse_args()
    
    print(args)
    if args.subcommand == 'examples':
        examples()
        sys.exit(0)
    return args

def main():
    global exact

    dotenv_file = os.getenv('SASHIMI_DOTENV', '.env')
    dotenv.load_dotenv(dotenv_file)
    # get_args()

    # exact = SashimiClient(base_url=args.server, project=args.project, token=args.token)
    
    app()
    sys.exit(0)

    try:
        if args.upload:
            upload()
        elif args.info:
            info()
        else:
            expr()
    except requests.RequestException as e:
        if e.response.status_code == 422:
            print(e.response.json())
        else:
            print("HTTP exception:", e)
        sys.exit(1)

if __name__ == '__main__':
    main()