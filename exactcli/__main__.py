import os
import json
import argparse
import dotenv
import sys
import requests

from . import ExactClient

args = None
exact: ExactClient = None


def check_arguments(fn: str):
    
    checks = {
        "upload": ["token", "ds"],
        "expr": ["token", "ds", "exprfilter"],
        "info": ["token"]
    }

    # checks common for all functions
    if not args.server:
        print("Need --server (or $EXACT_URL)")
        sys.exit(1)

    if not args.server:
        print("Need --server (or $EXACT_URL)")
        sys.exit(1)

    if not args.project:
        print("Need --project (or $EXACT_PROJECT)")
        sys.exit(1)


    # specific checks
    checklist = checks[fn]

    for ch in checklist:

        if ch == "ds":
            if args.ds is None:
                print("need --ds")
                sys.exit(1)

        elif ch == "token":
            if args.token is None:
                print("need --token (or $EXACT_TOKEN)")
                sys.exit(1)

        elif ch == "exprfilter":
            if args.token is None:
                print("need --expr or --filter")
                sys.exit(1)

        else:
            print(f"Do not know how to check {ch!r}")

def expr():

    # print(args.filter)
    check_arguments("expr")

    filter = {}
    if args.filter:
        for pair in args.filter:
            key, value = pair.split('=')
            filter[key] = json.loads(value)

    result = exact.query(args.ds, expr=args.expr, filter=filter, fields=args.fields, limit=args.limit, sort=args.sort, reverse=args.reverse)
    print(json.dumps(result, sort_keys=True, indent=4))


def info():
    check_arguments("info")
    result = exact.info()
    print(json.dumps(result, sort_keys=True, indent=4))


def upload():
    check_arguments('upload')

    # read file
    with open(args.upload) as fh:
        dataset = json.load(fh)

    if args.keypath:
        try:
            for key in args.keypath:
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
        result = exact.put(args.ds, dataset=dataset)
    except ValueError as e:
        print(e)
        sys.exit(1)

    # r = exact.query(ds_name, expr='True', limit=2)
    # print(len(r['result']))
    print(result)



def get_args():
    global args
    def_url = os.getenv('EXACT_URL')
    def_project = os.getenv('EXACT_PROJECT')
    def_dsname = os.getenv('EXACT_DSNAME')
    def_token = os.getenv('EXACT_TOKEN')
    
    parser = argparse.ArgumentParser(description='Exact CLI client')
    g = parser.add_argument_group('Target specification')
    g.add_argument('-s', '--server', metavar='SERVER', default=def_url, help='Exact server url, e.g. https://exact.www-security.com/')
    g.add_argument('-p', '--project', metavar='PROJECTNAME', default=def_project, help='Project url, e.g. https://exact.www-security.com/ds/sandbox')
    g.add_argument('--ds', metavar='DSNAME', default=def_dsname, help='Dataset name')
    g.add_argument('--token', metavar='TOKEN', default=def_token, help='Access token')

    g = parser.add_argument_group('Upload JSON')
    g.add_argument('-u', '--upload', metavar='file.json', help='File to upload')
    g.add_argument('--keypath', metavar='KEY', nargs="+", help='keypath (if needed)')

    g = parser.add_argument_group('Query dataset')
    g.add_argument('--expr', metavar='EXPRESSION', help='Expression like \'True\' or \'id==1\' or \'Brand == "Apple" and price < 1000\'')
    g.add_argument('--filter', nargs='*', metavar="KEY=VALUE", help='series key=value like: brand="Apple" price__lt=1000')
    g.add_argument('--limit', type=int, metavar='NUM', help='Limit')
    g.add_argument('--sort', metavar='FIELD', help='Sort by this field')
    g.add_argument('--reverse', default=False, action='store_true', help='reverse order for sort')
    g.add_argument('--fields', metavar='FIELD', nargs='+', help='return only these fields')

    g = parser.add_argument_group('Other')
    g.add_argument('--info', default=False, action='store_true', help='Get project info from server')

    args = parser.parse_args()
    return args

def main():
    global exact

    dotenv_file = os.getenv('EXACT_DOTENV', '.env')
    dotenv.load_dotenv(dotenv_file)
    get_args()

    exact = ExactClient(base_url=args.server, project=args.project, token=args.token)
    
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