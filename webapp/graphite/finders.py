import os
import fnmatch
from os.path import islink, isdir, isfile, realpath, join, dirname, basename
from glob import glob
from ceres import CeresTree, CeresNode, setDefaultSliceCachingBehavior
from graphite.node import BranchNode, LeafNode
from graphite.readers import CeresReader, WhisperReader, GzippedWhisperReader, RRDReader, KairosDBReader
from graphite.util import find_escaped_pattern_fields

from graphite.logger import log

import pyKairosDB
from pyKairosDB import graphite as pyk_graphite

#setDefaultSliceCachingBehavior('all')

class CeresFinder:
  def __init__(self, directory):
    self.directory = directory
    self.tree = CeresTree(directory)

  def find_nodes(self, query):
    for fs_path in glob( self.tree.getFilesystemPath(query.pattern) ):
      metric_path = self.tree.getNodePath(fs_path)

      if CeresNode.isNodeDir(fs_path):
        ceres_node = self.tree.getNode(metric_path)

        if ceres_node.hasDataForInterval(query.startTime, query.endTime):
          real_metric_path = get_real_metric_path(fs_path, metric_path)
          reader = CeresReader(ceres_node, real_metric_path)
          yield LeafNode(metric_path, reader)

      elif isdir(fs_path):
        yield BranchNode(metric_path)

class KairosDBFinder:
  """This class will take a path (e.g. "carbon.foo.bar") and return a
  node object.  If this is a leaf node (has no other names underneath
  it) then return a LeafNode object.  Otherwise, return  BranchNode object.

  """
  def __init__(self, kairosdb_server, kairosdb_port):
    """The directory argument seems to be un-necessary in this case, but it's part of the API I think

    :type directory: str
    :param directory: An un-needed argument that seems to be part of the API?
    """

    self.kairosdb_server=kairosdb_server
    self.kairosdb_port=kairosdb_port
    self.conn = pyKairosDB.connect(kairosdb_server, kairosdb_port) # XXX get connection info from configuration, use default for testing

  def find_nodes(self, query):
    """
    find_nodes is provided a query - an instance of a storage.FindQuery class.

    This is used to traverse the tree of metric names, returning a LeafNode if this
    is a leaf node, and a BranchNode if it is not.
    """
    print "Trying to find_nodes kairosdbfinder in finders"
    # print "metric paths are {0}".format(pyk_graphite.expand_graphite_wildcard_metric_name(self.conn, query.pattern))
    for metric_path in pyk_graphite.expand_graphite_wildcard_metric_name(self.conn, query.pattern):
      kind_of_node = pyk_graphite.leaf_or_branch(self.conn, metric_path)
      print "kind_of_node is {0}".format(kind_of_node)
      reader = KairosDBReader(self.conn, metric_path)
      if kind_of_node is "branch":
          yield BranchNode(metric_path)
      else: # it's "leaf"
          yield LeafNode(metric_path, reader)

class StandardFinder:
  DATASOURCE_DELIMETER = '::RRD_DATASOURCE::'

  def __init__(self, directories):
    self.directories = directories

  def find_nodes(self, query):
    clean_pattern = query.pattern.replace('\\', '')
    pattern_parts = clean_pattern.split('.')

    for root_dir in self.directories:
      for absolute_path in self._find_paths(root_dir, pattern_parts):
        if basename(absolute_path).startswith('.'):
          continue

        if self.DATASOURCE_DELIMETER in basename(absolute_path):
          (absolute_path, datasource_pattern) = absolute_path.rsplit(self.DATASOURCE_DELIMETER, 1)
        else:
          datasource_pattern = None

        relative_path = absolute_path[ len(root_dir): ].lstrip('/')
        metric_path = fs_to_metric(relative_path)
        real_metric_path = get_real_metric_path(absolute_path, metric_path)

        metric_path_parts = metric_path.split('.')
        for field_index in find_escaped_pattern_fields(query.pattern):
          metric_path_parts[field_index] = pattern_parts[field_index].replace('\\', '')
        metric_path = '.'.join(metric_path_parts)

        # Now we construct and yield an appropriate Node object
        if isdir(absolute_path):
          yield BranchNode(metric_path)

        elif isfile(absolute_path):
          if absolute_path.endswith('.wsp') and WhisperReader.supported:
            reader = WhisperReader(absolute_path, real_metric_path)
            yield LeafNode(metric_path, reader)

          elif absolute_path.endswith('.wsp.gz') and GzippedWhisperReader.supported:
            reader = GzippedWhisperReader(absolute_path, real_metric_path)
            yield LeafNode(metric_path, reader)

          elif absolute_path.endswith('.rrd') and RRDReader.supported:
            if datasource_pattern is None:
              yield BranchNode(metric_path)

            else:
              for datasource_name in RRDReader.get_datasources(absolute_path):
                if match_entries([datasource_name], datasource_pattern):
                  reader = RRDReader(absolute_path, datasource_name)
                  yield LeafNode(metric_path + "." + datasource_name, reader)

  def _find_paths(self, current_dir, patterns):
    """Recursively generates absolute paths whose components underneath current_dir
    match the corresponding pattern in patterns"""
    pattern = patterns[0]
    patterns = patterns[1:]
    entries = os.listdir(current_dir)

    subdirs = [e for e in entries if isdir( join(current_dir,e) )]
    matching_subdirs = match_entries(subdirs, pattern)

    if len(patterns) == 1 and RRDReader.supported: #the last pattern may apply to RRD data sources
      files = [e for e in entries if isfile( join(current_dir,e) )]
      rrd_files = match_entries(files, pattern + ".rrd")

      if rrd_files: #let's assume it does
        datasource_pattern = patterns[0]

        for rrd_file in rrd_files:
          absolute_path = join(current_dir, rrd_file)
          yield absolute_path + self.DATASOURCE_DELIMETER + datasource_pattern

    if patterns: #we've still got more directories to traverse
      for subdir in matching_subdirs:

        absolute_path = join(current_dir, subdir)
        for match in self._find_paths(absolute_path, patterns):
          yield match

    else: #we've got the last pattern
      files = [e for e in entries if isfile( join(current_dir,e) )]
      matching_files = match_entries(files, pattern + '.*')

      for basename in matching_files + matching_subdirs:
        yield join(current_dir, basename)


def fs_to_metric(path):
  dirpath = dirname(path)
  filename = basename(path)
  return join(dirpath, filename.split('.')[0]).replace('/','.')


def get_real_metric_path(absolute_path, metric_path):
  # Support symbolic links (real_metric_path ensures proper cache queries)
  if islink(absolute_path):
    real_fs_path = realpath(absolute_path)
    relative_fs_path = metric_path.replace('.', '/')
    base_fs_path = absolute_path[ :-len(relative_fs_path) ]
    relative_real_fs_path = real_fs_path[ len(base_fs_path): ]
    return fs_to_metric( relative_real_fs_path )

  return metric_path

def _deduplicate(entries):
  yielded = set()
  for entry in entries:
    if entry not in yielded:
      yielded.add(entry)
      yield entry

def match_entries(entries, pattern):
  """A drop-in replacement for fnmatch.filter that supports pattern
  variants (ie. {foo,bar}baz = foobaz or barbaz)."""
  v1, v2 = pattern.find('{'), pattern.find('}')

  if v1 > -1 and v2 > v1:
    variations = pattern[v1+1:v2].split(',')
    variants = [ pattern[:v1] + v + pattern[v2+1:] for v in variations ]
    matching = []

    for variant in variants:
      matching.extend( fnmatch.filter(entries, variant) )

    return list( _deduplicate(matching) ) #remove dupes without changing order

  else:
    matching = fnmatch.filter(entries, pattern)
    matching.sort()
    return matching
