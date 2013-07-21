
"""
A new reader class is created by creating a new-style class that has
the following methods:

  def get_intervals(self) -> returns: IntervalSet
  def fetch(self, startTime, endTime) -> returns: list of results

"""

import os
import time
from graphite.node import LeafNode, BranchNode
from graphite.intervals import Interval, IntervalSet
from graphite.carbonlink import CarbonLink
from graphite.logger import log
from django.conf import settings

try:
  import whisper
except ImportError:
  whisper = False

try:
  import rrdtool
except ImportError:
  rrdtool = False

try:
  import gzip
except ImportError:
  gzip = False

try:
  print "Trying to import pyKairosDB"
  import pyKairosDB
  from pyKairosDB import graphite as pyk_graphite
  kairos=True
except ImportError:
  kairos = False
  print "Failed to import pyKairosDB"


class FetchInProgress(object):
  def __init__(self, wait_callback):
    self.wait_callback = wait_callback

  def waitForResults(self):
    return self.wait_callback()


class MultiReader(object):
  __slots__ = ('nodes',)

  def __init__(self, nodes):
    self.nodes = nodes

  def get_intervals(self):
    interval_sets = []
    for node in self.nodes:
      interval_sets.extend( node.intervals.intervals )
    return IntervalSet( sorted(interval_sets) )

  def fetch(self, startTime, endTime):
    # Start the fetch on each node
    results = [ n.fetch(startTime, endTime) for n in self.nodes ]

    # Wait for any asynchronous operations to complete
    for i, result in enumerate(results):
      if isinstance(result, FetchInProgress):
        try:
          results[i] = result.waitForResults()
        except:
          log.exception("Failed to complete subfetch")
          results[i] = None

    results = [r for r in results if r is not None]
    if not results:
      raise Exception("All sub-fetches failed")

    return reduce(self.merge, results)

  def merge(self, results1, results2):
    # Ensure results1 is finer than results2
    if results1[0][2] > results2[0][2]:
      results1, results2 = results2, results1

    time_info1, values1 = results1
    time_info2, values2 = results2
    start1, end1, step1 = time_info1
    start2, end2, step2 = time_info2

    step   = step1                # finest step
    start  = min(start1, start2)  # earliest start
    end    = max(end1, end2)      # latest end
    time_info = (start, end, step)
    values = []

    t = start
    while t < end:
      # Look for the finer precision value first if available
      i1 = (t - start1) / step1

      if len(values1) > i1:
        v1 = values1[i1]
      else:
        v1 = None

      if v1 is None:
        i2 = (t - start2) / step2

        if len(values2) > i2:
          v2 = values2[i2]
        else:
          v2 = None

        values.append(v2)
      else:
        values.append(v1)

      t += step

    return (time_info, values)


class CeresReader(object):
  __slots__ = ('ceres_node', 'real_metric_path')
  supported = True

  def __init__(self, ceres_node, real_metric_path):
    self.ceres_node = ceres_node
    self.real_metric_path = real_metric_path

  def get_intervals(self):
    intervals = []
    for info in self.ceres_node.slice_info:
      (start, end, step) = info
      intervals.append( Interval(start, end) )

    return IntervalSet(intervals)

  def fetch(self, startTime, endTime):
    data = self.ceres_node.read(startTime, endTime)
    time_info = (data.startTime, data.endTime, data.timeStep)
    values = list(data.values)

    # Merge in data from carbon's cache
    try:
      cached_datapoints = CarbonLink.query(self.real_metric_path)
    except:
      log.exception("Failed CarbonLink query '%s'" % self.real_metric_path)
      cached_datapoints = []

    for (timestamp, value) in cached_datapoints:
      interval = timestamp - (timestamp % data.timeStep)

      try:
        i = int(interval - data.startTime) / data.timeStep
        values[i] = value
      except:
        pass

    return (time_info, values)


class KairosDBReader(object):
  __slots__ = ('server', 'port', 'conn', 'metric_path')
  supported = bool(kairos)

  def __init__(self, kairosdb_connection, metric_path):
    """
    :type kairosdb_connection: pyKairosDB.connection
    :param kairosdb_connection: This is a connection to the KairosDB serer

    :type metric_path: str
    :param metric_path: A string describing the metric name whose values are being sought
    """
    self.conn        = kairosdb_connection
    self.metric_path = metric_path

  def get_intervals(self):
    """What are intervals, exactly?

    XXX What I currently think I understand [PN] is that get_intervals
    returns a set (or rather, an IntervalSet) that contains Interval
    objects.  Each Interval is a start and end time.  When a request
    is finally made, it should be done by combining any overlapping
    time ranges (intervals), and then issuing a request for each range
    of times and those will be the stats.

    The start and end time appears to be normally generated by
    querying metadata about the metric(s) in question.  For instance,
    the whisper and rrd readers seem to be directly accessing
    properties of the respective databases files (st_mtime) to construct a period that
    runs from now, to the oldest possible retained value in the file
    store for this particular metric.

    It seems, then, that it's acceptabe to wait a moment here while we
    go off to the server and get the oldest datapoint for a particular
    metric, so I have to figure out what query options I have to
    satisfy this.

    """
    intervals = []

    print "Interval finding is waiting on https://code.google.com/p/kairosdb/issues/detail?id=12"
    #for info in self.ceres_node.slice_info:
    #  (start, end, step) = info
    #  intervals.append( Interval(start, end) )
    #
    # For now, use a bogus interval
    intervals.append(Interval(time.time()-3600, time.time()))
    return IntervalSet(intervals)

  def fetch(self, startTime, endTime):
    """This will get called for each range.  This seems sub-optimal
    since KariosDB can satisfy multiple query ranges in one
    request, but that can come later.

    This is called from datalib.fetchData() via LeafNode

    :type startTime: float
    :param startTime: The time in seconds since the unix epoch that we want to start fetching metrics

    :type endTime: float
    :param endTime: The time in seconds since the unix epoch that we want bound our fetch at

    :rtype: tuple
    :return: A tuple of (time_info, values).  Time_info is a tuple of (start, end, interval)
             where interval is seconds between metrics.  Values is a list of floats which are
             the values to be graphed.
    """
    # expanded_metric_names = pyk_graphite.expand_graphite_wildcard_metric_name(self.conn, self.real_metric_path)
    # data = self.conn.read_absolute(expanded_metric_names, startTime, endTime)
    (time_info, values) = pyk_graphite.read_absolute(self.conn, self.metric_path, startTime, endTime)

    # I think we'll have to infer a timestep for each metric -
    # probably find the max # of datapoints in any particular returned set,
    # and divide the time evenly between all of those.
    # time_info = (data.startTime, data.endTime, data.timeStep)
    # values = list(data.values)

    return (time_info, values)


class WhisperReader(object):
  __slots__ = ('fs_path', 'real_metric_path')
  supported = bool(whisper)

  def __init__(self, fs_path, real_metric_path):
    self.fs_path = fs_path
    self.real_metric_path = real_metric_path

  def get_intervals(self):
    start = time.time() - whisper.info(self.fs_path)['maxRetention']
    end = max( os.stat(self.fs_path).st_mtime, start )
    return IntervalSet( [Interval(start, end)] )

  def fetch(self, startTime, endTime):
    data = whisper.fetch(self.fs_path, startTime, endTime)
    if not data:
      return None

    time_info, values = data
    (start,end,step) = time_info

    # Merge in data from carbon's cache
    try:
      cached_datapoints = CarbonLink.query(self.real_metric_path)
    except:
      log.exception("Failed CarbonLink query '%s'" % self.real_metric_path)
      cached_datapoints = []

    for (timestamp, value) in cached_datapoints:
      interval = timestamp - (timestamp % step)

      try:
        i = int(interval - start) / step
        values[i] = value
      except:
        pass

    return (time_info, values)


class GzippedWhisperReader(WhisperReader):
  supported = bool(whisper and gzip)

  def get_intervals(self):
    fh = gzip.GzipFile(self.fs_path, 'rb')
    try:
      info = whisper.__readHeader(fh) # evil, but necessary.
    finally:
      fh.close()

    start = time.time() - info['maxRetention']
    end = max( os.stat(self.fs_path).st_mtime, start )
    return IntervalSet( [Interval(start, end)] )

  def fetch(self, startTime, endTime):
    fh = gzip.GzipFile(self.fs_path, 'rb')
    try:
      return whisper.file_fetch(fh, startTime, endTime)
    finally:
      fh.close()


class RRDReader:
  supported = bool(rrdtool)

  def __init__(self, fs_path, datasource_name):
    self.fs_path = fs_path
    self.datasource_name = datasource_name

  def get_intervals(self):
    start = time.time() - self.get_retention(self.fs_path)
    end = max( os.stat(self.fs_path).st_mtime, start )
    return IntervalSet( [Interval(start, end)] )

  def fetch(self, startTime, endTime):
    startString = time.strftime("%H:%M_%Y%m%d+%Ss", time.localtime(startTime))
    endString = time.strftime("%H:%M_%Y%m%d+%Ss", time.localtime(endTime))

    if settings.FLUSHRRDCACHED:
      rrdtool.flushcached(self.fs_path, '--daemon', settings.FLUSHRRDCACHED)

    (timeInfo, columns, rows) = rrdtool.fetch(self.fs_path,'AVERAGE','-s' + startString,'-e' + endString)
    colIndex = list(columns).index(self.datasource_name)
    rows.pop() #chop off the latest value because RRD returns crazy last values sometimes
    values = (row[colIndex] for row in rows)

    return (timeInfo, values)

  @staticmethod
  def get_datasources(fs_path):
    info = rrdtool.info(fs_path)

    if 'ds' in info:
      return [datasource_name for datasource_name in info['ds']]
    else:
      ds_keys = [ key for key in info if key.startswith('ds[') ]
      datasources = set( key[3:].split(']')[0] for key in ds_keys )
      return list(datasources)

  @staticmethod
  def get_retention(fs_path):
    info = rrdtool.info(fs_path)
    if 'rra' in info:
      rras = info['rra']
    else:
      # Ugh, I like the old python-rrdtool api better..
      rra_count = max([ int(key[4]) for key in info if key.startswith('rra[') ]) + 1
      rras = [{}] * rra_count
      for i in range(rra_count):
        rras[i]['pdp_per_row'] = info['rra[%d].pdp_per_row' % i]
        rras[i]['rows'] = info['rra[%d].rows' % i]

    retention_points = 0
    for rra in rras:
      points = rra['pdp_per_row'] * rra['rows']
      if points > retention_points:
        retention_points = points

    return  retention_points * info['step']
