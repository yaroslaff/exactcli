# exactcli

## Install

~~~
pipx install sashimi
~~~

But you may use pip as well.

## Config example

Config file is either `.env` in current directory or override it with `SASHIMI_DOTENV=/path/file` env variable.

config example:
~~~
SASHIMI_PROJECT=http://localhost:8000/ds/sandbox
SASHIMI_TOKEN=envtoken
~~~
These defaults will make your commands shorter.

## Database support
~~~
# postgresql
pip install psycopg2

# mariadb / mysql
pip install mysqlclient
~~~

## Usage

Get info about your project and datasets
~~~
exactcli --info
~~~

Upload new JSON file to project
~~~
exactcli --upload ~/repo/exact/tests/products.json --ds products --keypath products
~~~

Query it
~~~
exactcli --ds products --expr 'True' --sort price --limit 1
exactcli --ds products --expr 'brand=="Apple" and price<1000' --fields id brand title description price
~~~

~~~
exactcli --ds products --filter brand=\"Apple\" price__lt=2000
exactcli --ds products --filter 'brand="Apple"' price__lt=2000
~~~
