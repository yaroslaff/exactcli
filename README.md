# sashimi-cli

CLI tool and python module to work with Sashimi headless CMS

## Install

If you need just CLI tool (and do not plan to write python programs working with Sashimi) use this
~~~
pipx install sashimi-cli
~~~

But you may use pip as well. This will install minimal version (enough to work with JSON)

### Database support

Sqlite3 support is built-in. To use with 'real' databases, install optional dependencies:
~~~
pipx install sashimi-cli[mysql]
pipx install sashimi-cli[postgresql]
~~~

You will need this to import SQL query results to Sashimi.

## Config example

Config file is either `.env` in current directory or override it with `SASHIMI_DOTENV=/path/file` env variable.

config example:
~~~
SASHIMI_PROJECT=http://localhost:8000/ds/sandbox
SASHIMI_TOKEN=envtoken
~~~
These defaults will make your commands shorter.

## Help

To get help, run `sashimi -h` or `sashimi --help`. Sashimi has many subcommands, each subcommand has it's own help, e.g.  `sashimi query --help` or `sashimi upload -h`.

## Quick start
Create config (as shown above) and run simple query commands like:

Simple queries:
~~~shell
# get project by id
sashimi query products id=1
{
    'status': 'OK',
    'limit': 20,
    'matches': 1,
    'truncated': False,
    'exceptions': 0,
    'last_exception': None,
    'result': [
        {
            'id': 1,
            'title': 'iPhone 9',
            'description': 'An apple mobile which is nothing like apple',
            'price': 549,
            'discountPercentage': 12.96,
            'rating': 4.69,
            'stock': 94,
            'brand': 'Apple',
            'category': 'smartphones',
            'thumbnail': 'https://i.dummyjson.com/data/products/1/thumbnail.jpg',
            'images': [
                ...
            ]
        }
    ],
    'time': 0.0
}

# find 5 cheapest smartphones, get only title and price
sashimi query products 'category="smartphones"' --limit=5 --sort=price -f title -f price
{   
    'status': 'OK',
    'limit': 5,
    'matches': 5,
    'truncated': False,
    'exceptions': 0,
    'last_exception': None,
    'result': [
        {'title': 'OPPOF19', 'price': 280},
        {'title': 'Huawei P30', 'price': 499},
        {'title': 'iPhone 9', 'price': 549},
        {'title': 'iPhone X', 'price': 899},
        {'title': 'Samsung Universe 9', 'price': 1249}
    ],
    'time': 0.0
}

# complex query, all power of Pythonic expressions. We do not like OPPO and Xiaomi, and we look for smartphone with high rating or Apple (with any rating)
# -e to specify pythonic expression, -r to show only search result, no special fields
sashimi query products -e 'category=="smartphones" and brand not in ["OPPO", "Xiaomi"] and (rating>4 or brand=="Apple")' -f brand -f title -f price -r
[
    {'brand': 'Apple', 'title': 'iPhone 9', 'price': 549},
    {'brand': 'Apple', 'title': 'iPhone X', 'price': 899},
    {'brand': 'Samsung', 'title': 'Samsung Universe 9', 'price': 1249},
    {'brand': 'Huawei', 'title': 'Huawei P30', 'price': 499}
]
~~~

aggregations:
~~~shell
# all categories in our marketplace
sashimi query products -a 'distinct:category' -d
{
    ...
    'aggregation': {
        'distinct:category': [
            'automotive',
            'fragrances',
            ...
            'womens-watches'
        ]
    },
    'time': 0.0
}

# how many total smartphones we have on stock, what is min and max price
sashimi query products 'category="smartphones"' -a min:price -a max:price -a sum:stock -a distinct:brand -d
{
    ...
    'aggregation': {'min:price': 280, 'max:price': 1249, 'sum:stock': 319, 'distinct:brand': ['Apple', 'Huawei', 'OPPO', 'Samsung']},
}
~~~

upload your JSON:
~~~shell
sashimi upload samples/products.json p2
~~~

Import data from your database
~~~shell
# Import our books database to Sashimi dataset `libro`
$ sashimi import mysql://xenon@localhost/libro 'SELECT * FROM libro' libro  
Processing... ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100% 0:00:01
# Loaded from db dataset of 57554 records in 2.77 seconds
Loaded dataset 'libro' (57554 records)

~~~
you can import tables (SELECT * FROM table), subset of records/fields (SELECT ... WHERE ...) or any complex SELECT query with JOINs, etc.

