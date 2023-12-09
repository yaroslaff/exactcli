# exactcli

## Install

~~~
pipx exactcli
~~~

But you may use pip as well.

## Config example

Config file is either `.env` in current directory or override it with `EXACT_DOTENV=/path/file` env variable.

config example:
~~~
EXACT_URL=http://localhost:8000/
EXACT_PROJECT=sandbox
EXACT_TOKEN=envtoken
EXACT_DSNAME=products
~~~
These defaults will make your commands shorter.

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
