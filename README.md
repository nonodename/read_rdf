# ReadRdf

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

---

This extension, ReadRdf, allow you to read RDF files directly into DuckDB. The [SERD](https://drobilla.gitlab.io/serd/doc/singlehtml/) libray is used for this, meaning the extension can parse [Turtle](http://www.w3.org/TR/turtle/), [NTriples](http://www.w3.org/TR/n-triples/), [NQuads](http://www.w3.org/TR/n-quads/), and [TriG](http://www.w3.org/TR/trig/).

Six columns are returned for RDF. Graph, Subject, predicate, object, language_tag (if present), datatype (if present).


## Building
### Managing dependencies
This project doesn't currently use VCPKG so all discussion of it removed. You don't need  that for build :-)

### Build steps
To build the extension, first clone this repo. Then in the repo base locally run:

```sh
git submodule update --init --recursive
```
To get the source for DuckDB and CI-tools. Next run: 

```sh
make
```
If you have ninja avilable you can use that for faster builds:
```sh
GEN=ninja make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/read_rdf/read_rdf.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `read_rdf.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

Now we can use the features from the extension directly in DuckDB. The template contains a single table function `read_rdf()` that takes a single string arguments (the name of the NTriples file) and returns a table:
```
D select * from read_rdf('tests.nt');
┌──────────────────────┬──────────────────────┬──────────────────────┬──────────────┬────────────────────────────┐
│       subject        │      predicate       │        object        │ language_tag │        datatype_iri        │
│       varchar        │       varchar        │       varchar        │   varchar    │          varchar           │
├──────────────────────┼──────────────────────┼──────────────────────┼──────────────┼────────────────────────────┤
│ http://example.org…  │ http://www.w3.org/…  │ http://xmlns.com/f…  │ NULL         │ NULL                       │
│ http://example.org…  │ http://xmlns.com/f…  │ John Doe             │ NULL         │ NULL                       │
│ http://example.org…  │ http://xmlns.com/f…  │ 30                   │ NULL         │ http://www.w3.org/2001/X…  │
│ http://example.org…  │ http://xmlns.com/f…  │ jane                 │ NULL         │ NULL                       │
│ jane                 │ http://www.w3.org/…  │ http://xmlns.com/f…  │ NULL         │ NULL                       │
│ jane                 │ http://xmlns.com/f…  │ Jane Smith           │ en           │ NULL                       │
│ http://example.org…  │ http://purl.org/dc…  │ The Great Book       │ NULL         │ NULL                       │
│ http://example.org…  │ http://purl.org/dc…  │ http://example.org…  │ NULL         │ NULL                       │
│ http://unicode.org…  │ http://example.org…  │ 🦆                   │ NULL         │ NULL                       │
└──────────────────────┴──────────────────────┴──────────────────────┴──────────────┴────────────────────────────┘
```

## Running the tests
Test for this extension are SQL tests in `./test/sql`. They rely on a sample triples file `tests.nt` These SQL tests can be run using:
```sh
make test
```

### Installing the deployed binaries
To install from GitHub actions:
* navigate to the [actions](https://github.com/nonodename/read_rdf/actions) for this repo
* click on the latest successful build
* select the architecture you want from the left hand navigation
* open the `Run actions/upload artifact` step
* find the artifact URL for the compiled extension
* download, unzip and then [install](https://duckdb.org/docs/stable/extensions/advanced_installation_methods) to DudkDB

To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL read_rdf
LOAD read_rdf
```
