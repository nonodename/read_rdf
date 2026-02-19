# Read_Rdf

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

---

This extension, Read_Rdf, allow you to read RDF files directly into DuckDB. The [SERD](https://drobilla.gitlab.io/serd/doc/singlehtml/) libray is used for this, meaning the extension can parse [Turtle](http://www.w3.org/TR/turtle/), [NTriples](http://www.w3.org/TR/n-triples/), [NQuads](http://www.w3.org/TR/n-quads/), and [TriG](http://www.w3.org/TR/trig/). An experimental parser is also provideded for RDF/XML serialization. This is used when the file extension is `.rdf` or `.xml`.

Six columns are returned for RDF. Three are always not null:
* subject
* predicate
* object

The other three columns will be null if no value is provided in the underlying RDF file:
* graph
* language_tag
* datatype

## Building
### Managing dependencies
DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the installation instructions or just run the following:

```sh
cd <your-working-dir-not-the-plugin-repo>
git clone https://github.com/Microsoft/vcpkg.git
cd vcpkg && git checkout ce613c41372b23b1f51333815feb3edd87ef8a8b
sh ./scripts/bootstrap.sh -disableMetrics
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

### Build steps
To build the extension, first clone this repo. Then in the repo base locally run:

```sh
git submodule update --init --recursive
```
(That command also works for updating to the latest version of the submodules) 
To get the source for DuckDB, Serd and CI-tools. Next run: 

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

Now we can use the features from the extension directly in DuckDB. `read_rdf()` takes a file path or glob pattern and returns a table. When a glob pattern matches multiple files, all matching files are read and their triples are combined:
```
D select subject, predicate from read_rdf('test/rdf/tests.nt');
┌───────────────────────────────────┬─────────────────────────────────────────────────┐
│              subject              │                    predicate                    │
│              varchar              │                     varchar                     │
├───────────────────────────────────┼─────────────────────────────────────────────────┤
│ http://example.org/person/JohnDoe │ http://www.w3.org/1999/02/22-rdf-syntax-ns#type │
│ http://example.org/person/JohnDoe │ http://xmlns.com/foaf/0.1/name                  │
│ http://example.org/person/JohnDoe │ http://xmlns.com/foaf/0.1/age                   │
│ http://example.org/person/JohnDoe │ http://xmlns.com/foaf/0.1/knows                 │
│ jane                              │ http://www.w3.org/1999/02/22-rdf-syntax-ns#type │
│ jane                              │ http://xmlns.com/foaf/0.1/name                  │
│ http://example.org/book/123       │ http://purl.org/dc/elements/1.1/title           │
│ http://example.org/book/123       │ http://purl.org/dc/elements/1.1/creator         │
│ http://unicode.org/duck           │ http://example.org/hasEmoji                     │
└───────────────────────────────────┴─────────────────────────────────────────────────┘
```
### Optional Parameters

#### Strict Parsing

The optional parameter `strict_parsing`, defaults to true and exposes the underlying strict parsing feature of the serd RDF parsing library. When false it permits malformed URIs. To disable strict parsing, pass `strict_parsing = false`.

#### Prefix Expansion

The optional parameter `prefix_expansion` defaults to false and exposes the underlying serd `serd_env_expand_node` function to expand [CURIE](https://en.wikipedia.org/wiki/CURIE) form URIs to fully defined URIs. This is applied to all columns and is ignored when parsing ntriples and nquads.

#### File Type override

The optional parameter `file_type` can be used to override the detected file type of the file. The following values are recognized:
 * Turtle: `ttl`, `turtle`
 * NQuads: `nq`, `nquads`
 * NTriples: `nt`, `ntriples`
 * Trig: `trig`
 * RDF/XML `rdf`, `xml`

When using a glob pattern the `file_type` override is applied uniformly to every matched file.

### Glob / multiple files

The path argument accepts glob patterns, allowing multiple RDF files to be read in a single call. All matched files are scanned in parallel and their triples are combined into one result set:

```sql
-- Read all NTriples files in a directory
SELECT COUNT(*) FROM read_rdf('data/shards/*.nt');

-- Mix with other parameters — file_type applies to every matched file
SELECT * FROM read_rdf('data/shards/*.dat', file_type = 'ttl', strict_parsing = false);
```

If the pattern matches no files an `IO Error` is raised.

## Running the tests
Test for this extension are SQL tests in `./test/sql`. They rely on a samples in the test/rdf directory. These SQL tests can be run using:
```sh
make test
```

### Installing the deployed binaries
To install from GitHub actions:
* navigate to the [actions](https://github.com/nonodename/read_rdf/actions) for this repo
* click on the latest successful build (or build for a release)
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

If you'd like to see this listed as a community extension, please file an issue (or comment on an existing issue for the same) and if there's sufficient demand I'll try and make it happen.

## Reporting bugs

Please report bugs as issues on this project. Provide a sample RDF file that demonstrates the bug as well as steps to reproduce.