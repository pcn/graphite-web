# Overview

Graphite is a system built around a really strong graphing front-end.
The very strong front-end is supported by a very simple model for its
back-end.

Graphite (that is both carbon and graphite-web) was built around using
whisper and/or RRD as its datastores.  These are very space-efficient
tools to store metric data, and then downsample that data in a
granular manner.

The primary simplicity that they impose is that the data that you put
in cannot have any flexibility, and it must strictly follow the input
policy, the downsampling policy, and the eviction policy that the
metric file was created with.

There is also ceres.  It is an improvement, but it doesn't necessarily
handle the scaling as well as some other solutions.

# Whisper: The weakness in the simplicity

There are some glaring issues with the simplicity.  We'll ignore rrd,
since whisper is really the storage system that graphite is reliant on
- rrd is almost never used.

## Whisper: Pre-allocating storage

Because whisper's storage format relies on a fixed-size file, and on
offsets within that file to judge the time of a datapoint, you need to
allocate all storage up front.  This means that if you have a lot of
metrics being created, it is both slow, and a very inefficient use of
I/O on the server.

## Whisper: need to inefficiently search around the filesystem

Since whisper is based upon files on the filesystem, if there are many
launches of nodes (e.g. via a cloud provider) or of software (e.g. if
version of software is in the metric name) then looking up wildcards
will be expensive.  This is because in order to determine whether a
file contains relevant metrics to a query, every file that matches a
wildcard must be opened in order to see whether there is data for the
time period specified.  This means that a directory traversal, an
open, a read of metadata, and a seek and another read needs to be done
in order to determine that a metric does or does not exist within a
given timeframe.

## Whisper: storage is in fixed-size buckets

There is no flexabilty in the resolution at which data is collected
and reported.  If I send data at a higher resolution I will lose all
of those data points but the last one.  This is a perennial problem
with people following etsy's "graph everything" post.

# Pluggable time series interfaces - a solution

Whisper is great as a super-simple, no-starch way of getting graphite
up and running and evaluating it, and if you don't grow much, it could
do everything you need. Ceres probably is too.  I'm not as familiar
with it.

However, a real time series database would be an ideal step up because
the current generation now have the following qualities:

* cheap metric creation
* maintenance functions to grow and shrink clusters
* aggregation functions to provide bucketed data
* add and remove metrics via http (json)

The only thing that's necessary is an API and an implementation.

# The API proposal

## prelude

First, for both carbon and graphite-web, There will be a configuration
option:

```
ENABLE_STORAGE_BACKEND = <string> [, <string>, ...]
```

This will default to the empty string "".  If it is empty, then nothing will be done.

If it is not an empty string, then it will be stripped of whitespaces
and split around commans, and turned into an ordered set of distinct strings.
For each element of the ordered set, a matching configuration section must
appear in the config file (e.g. carbon.conf, local-settings.py).

So for example the configuration

```
ENABLE_STORAGE_BACKEND =
```

will result in no backends being loaded.

The configuration

```
ENABLE_STORAGE_BACKEND = kairosdb
```

will result in an OrderedSet('kairosdb'), and reqires that a matching
configuration section "[kairosdb]" exist (in carbon.conf) or a
dictionary called "kairosdb" exist (in local-settings.py).

The configuration

```
ENABLE_STORAGE_BACKEND = kairosb, influxdb
```

requires that matching configuration sections exist (e.g. "[kairosdb]"
and "[influxdb]" in carbon.conf, and a dictionary called "kairosdb"
and a dictionary called "influxdb" in local-settings.py).

The mandatory section config vars for each backend are:

* MODULE_NAME = <string>
* CONNECT_PARAMS = <string>
* BATCH_PARAMS = <int>
* WRITE_BATCH_PARAMS = <string>

The MODULE_NAME will be used to import that module.  The name
"MODULE_NAME.graphite" must exist, and will be imported and used.

Neither the module nor the graphite package within it should expect to
be imported with a "import *" as that is undesireable and will result
in naming conflicts.


The CONNECT_PARAMS will be a string consisting of comma-separated
key:value pairs that will be stripped of whitespace, split around the
commas, and be turned into keys and values that will be passed into
the module's connect method() as **kwargs.

The BATCH_PARAM will be the count of metrics that each batch contains

The WRITE_BATCH_PARAMS will provide the number of retries that the
write will perform before giving up.  There will be an exponential, or
at least greater-than-linear time backoff between retries.  The
increase may taper off to a reasonable max.

## For carbon

Once a module's graphite package has been imported, the following functions must be defined in the graphite package:

connect(**connect_params) -> backend-specific connection object
    The specific parameters are specific to the backend but would
    normally contain at least host, port, user, etc. depending on the
    backend

yield_batch(file_object, metrics_per_batch) -> batch of metrics.
    This function that takes a file or file-like object, and yields an
    object containig metrics_per_batch # of metrics, formatted
    apropriately for write_metrics.

write_batch_with_retry(connection, batch, retry_count) ->

    backend-specific result code that can be evaluated for truthiness.
    For both kairosdb and influxdb, this will be a requests object's
    result code.


## For graphite-web

These sections each relate to one major part of the graphite webapp
that will need to have functionality added to support pluggable
backends.  In each section, the necessary

### Config


#### Finders
* FINDER_CLASS = <string>
    e.g. FINDER_CLASS = pyKairosDB.graphite.KairosDBFinder

The FINDER_CLASS is the name of the class of the finder that will be
implemented for this backend.
methods:

A finder must implement

* __init__(self, connection) -> self
    The constructor takes a backend-appropriate connection object.


* find_nodes(self, query) -> Subclass of a Node - either BranchNode or LeafNode.

    a method where the query is a dot-separated hierarchy with "*" for
    wildcards.  A node that has sub-nodes branching from it cannot
    contain data.

#### Readers
* READER_CLASS = <string>
    e.g. READER_CLASS = pyKairosDB.graphite.KairosDBReader

The READER_CLASS is the name of the class of the reader that will
fetch data for this backend.

methods:

A reader must implement

* __init__(self, connection, metric_path) -> self

* get_intervals(self)
    This method is specific to ceres, and maybe can be depricated later from other backends.
    Return some bogus interval for now (e.g.
    ```
    intervals = []

    # Interval finding is waiting on https://code.google.com/p/kairosdb/issues/detail?id=12
    # For now, use a bogus interval - it doesn't seem to be that important as long as there is one?
    # That's right, this is an internal detail to ceres, and outside of ceres nothing else should
    # care about it.
    intervals.append(Interval(time.time()-3600, time.time()))
    return IntervalSet(intervals)
    ```

* fetch(self, startTime, endTime) -> tuple of (timeinterval, [values])


# TODO

There is still work to do on making sure that the configuration will
work.  This is a slimmed-down version of things that work for
pyKairosDB right now, and I intend to first get influxdb working as
identically as possible to kairosdb, then validate that this API will
work, including having multiple backends for graphite-web.

This is spec'd to work with the spooling relay right now.  I don't
think this proposal by itself will make the stock carbon-relay perform
well enough to be a high-volume relay.