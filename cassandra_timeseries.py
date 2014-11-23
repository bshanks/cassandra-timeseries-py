import logging
import sys
from threading import Lock
from datetime import timedelta
from datetime import datetime as dtm

from decorator import decorator
from numpy import datetime64
import pycassa
from pycassa.types import CompositeType, UTF8Type, LongType, DoubleType, BooleanType
from pycassa.pool import ConnectionPool
from pycassa.system_manager import SystemManager
from pycassa.columnfamily import ColumnFamily
from pycassa import NotFoundException
from numpy import argmax, isnan
import re
from itertools import ifilter




# numpy.argmin broken on dates: http://projects.scipy.org/numpy/ticket/1149
def argmin(listable):
    xs = list(listable)
    current_min = xs[0]
    current_min_loc = 0
    for i in range(len(xs)):
        item = xs[i]
        if item < current_min:
            current_min = item
            current_min_loc = i
        try:
            if isnan(item):
                current_min = item
                current_min_loc = i
        except TypeError:
            pass
    return current_min_loc


# a.selectTimeInterval('i','f','d',datetime.datetime.utcnow() - datetime.timedelta(60), ,datetime.datetime.utcnow())

# note: using half-open intervals [begin, end)

## class Datum(IsDescription):
##     time      = Int64Col()
##     value     = FloatCol()


## class IntervalObservation(IsDescription):
##     begin_time = Int64Col()
##     end_time      = Int64Col()
##     timestamp      = Int64Col()
    
##     confidence = FloatCol()
##     status = StringCol(16)
##     source = StringCol(64)
##     comment = StringCol(255)

itemTimeCompositeType = CompositeType(UTF8Type(), LongType())
itemRevTimeCompositeType = CompositeType(UTF8Type(), LongType(reversed=True))  # actually i don't think the "reversed" matters here since this is a key comparator
itemRevTimeTimeCompositeType = CompositeType(UTF8Type(), LongType(reversed=True), LongType())
# according to https://github.com/pycassa/pycassa/blob/master/pycassa/types.py , LongType is an 8-byte integer, like (presumably) datetime64

#note http://pycassa.github.com/pycassa/assorted/composite_types.html#inclusive-or-exclusive-slice-ends
    
class CassandraTimeSeries(object):


    def __init__(self, keyspace, shouldFullyIndex=True, warn=logging.warn, column_family_op_options={}, shouldLog=True, *args, **kwds):
            self._warn = warn
            self._keyspace = keyspace
            if shouldLog:
                logging.debug('Attempting to connect to Cassandra keyspace %s' % keyspace)
            systemManager = pycassa.system_manager.SystemManager(timeout=300) # todo lower crazy timeout
        
            keyspaces = systemManager.list_keyspaces()
            if keyspace not in keyspaces:
                if shouldLog:
                    logging.debug('Creating Cassandra keyspace %s' % keyspace)
                systemManager.create_keyspace(keyspace, strategy_options={"replication_factor": "1", "partitioner": "org.apache.cassandra.dht.ByteOrderedPartitioner"})
            #self._pool = ConnectionPool(keyspace, pool_size=256, pool_timeout=12000, timeout=6000, max_overflow=256) # todo lower crazy timeout
            logging.debug('Connected to Cassandra keyspace %s' % keyspace)
            self._pool = ConnectionPool(keyspace, pool_size=5, pool_timeout=450, timeout=300, max_overflow=5, max_retries=5) # todo lower crazy timeout
            self._column_family_op_options = column_family_op_options
            self._shouldLog = shouldLog

    def _getColumnFamily(self, duration, table_type):
        # todo: non-cryptic error message if it doesn't exist
        #return ColumnFamily(self._pool, self._columnFamilyName(duration, table_type), **self._column_family_op_options)
        return ColumnFamily(self._pool, self._columnFamilyName(duration, table_type), **self._column_family_op_options)

    @staticmethod
    def _columnFamilyName(duration, tableType):
        return re.sub('\.','_',str(duration.total_seconds())) + '_' + tableType


    def _createColumnFamily_interval_observations_or_manual_index(self, systemManager, existing_column_families, columnFamilyName, key_validation_class, fields, alter_existing_columns=False):
        if not columnFamilyName in existing_column_families:
            if self._shouldLog:
                logging.debug('Creating Cassandra column family %s' % columnFamilyName)
            systemManager.create_column_family(self._keyspace, columnFamilyName, key_validation_class=key_validation_class, default_validation_class=DoubleType())

        if not columnFamilyName in existing_column_families or alter_existing_columns:

                # note: due to lack of isolation in cassandra you may get an entry with some as-yet unfilled columns...
                #   todo should use begin_time, end_time in keys, not in columns
            interval_observation_field_column_validators = {'begin_time': LongType(), 'end_time': LongType(), 'status': UTF8Type(), 'source': UTF8Type(), 'confidence': DoubleType(), 'comment': UTF8Type()}
            for field in fields:
                interval_observation_field_column_validators[field]= BooleanType()

        #for column in interval_observation_field_column_validators:
        #    #print '***: %s %s %s %s' % (self._keyspace, columnFamilyName, column, interval_observation_field_column_validators[column]) 
        #    systemManager.alter_column(self._keyspace, columnFamilyName, column, interval_observation_field_column_validators[column])

            if self._shouldLog:
                logging.debug('Setting column validators in Cassandra column family %s' % columnFamilyName)

            systemManager.alter_column_family(self._keyspace, columnFamilyName, column_validation_classes=interval_observation_field_column_validators)
        

    def init_durations_and_fields(self, durations=[timedelta(0)], fields=[], columnFamilyOptions={}, alter_existing_columns=False):
        #print "*** INIT STARTED: %s %s" % (durations, fields)

        systemManager = pycassa.system_manager.SystemManager(timeout=600) # todo lower crazy timeout
        columnFamilies = systemManager.get_keyspace_column_families(self._keyspace)

        for duration in durations:
            columnFamilyName = self._columnFamilyName(duration, 'main')

            if duration == timedelta(0):
                allColumnFamilyOptions = {'default_validation_class' : DoubleType(), 'key_cache_size': 2000, 'row_cache_size': 200}
                allColumnFamilyOptions.update(columnFamilyOptions)
            else:
                allColumnFamilyOptions = {'default_validation_class' : DoubleType(), 'key_cache_size': 200000, 'row_cache_size': 20000}
                allColumnFamilyOptions.update(columnFamilyOptions)


            if not columnFamilyName in columnFamilies:
                if self._shouldLog:
                    logging.debug('Creating Cassandra column family %s' % columnFamilyName)
                systemManager.create_column_family(self._keyspace, columnFamilyName, key_validation_class=itemTimeCompositeType, **allColumnFamilyOptions)

                self._createColumnFamily_interval_observations_or_manual_index(systemManager, columnFamilies, self._columnFamilyName(duration, 'interval_observations'), itemRevTimeTimeCompositeType, fields, alter_existing_columns=alter_existing_columns)
                self._createColumnFamily_interval_observations_or_manual_index(systemManager, columnFamilies, self._columnFamilyName(duration, 'begin_time_manual_index'), itemRevTimeCompositeType, fields, alter_existing_columns=alter_existing_columns)
                self._createColumnFamily_interval_observations_or_manual_index(systemManager, columnFamilies, self._columnFamilyName(duration, 'end_time_manual_index'), itemTimeCompositeType, fields, alter_existing_columns=alter_existing_columns)

        #print "*** INIT FINISHED: %s %s" % (durations, fields)


            
    
    @staticmethod
    def _makeKey(item, time):
        return (unicode(item), datetime64(time).astype(int))


    @staticmethod
    def _makeIntervalKey(item, beginTime, endTime):
        return (unicode(item), datetime64(beginTime).astype(int), datetime64(endTime).astype(int))

    # todo make __getitem__ with key as a tuple
    def get_nodefault(self, item, field, time, duration):
                cf = self._getColumnFamily(duration, 'main')
                try:
                    return cf.get(self._makeKey(item, time), columns=[field])[field]
                except NotFoundException:
                    raise KeyError('CassandraTimeSeries: get_nodefault not found')

    def get(self, item, field, time, duration, default=None):
        try:
            return self.get_nodefault(item, field, time, duration)
        except KeyError:
            return default
    
    # does not return a generator b/c the whole point of this fn is to save time by
    # doing the query in one trip
    def multiget(self, item, field, times, duration, default=None, useNoneAsDefault=False):  # pass None to fields for all todo reorder, standardize ordering
        cf = self._getColumnFamily(duration, 'main')
        keys = [self._makeKey(item, time) for time in times]
        #print len(keys)
        #print 'keys: %s' % keys
        #print cf.multiget(keys, columns=[field])
        resultDict = cf.multiget(keys, columns=[field])
        # NOTE: if you multiget a bunch of the same keys, you don't get repeated entries
        #       also, nonexistent keys will just fail to be in the output OrderedDict, rather than
        #       raising an exception
        #  so, if we want to preserve duplicates and raise an exception, we have to do an extra step
        try:
            if default is None and not useNoneAsDefault:
                return [resultDict[key][field] for key in keys]
            else:
                return [resultDict.get(key, {}).get(field, default) for key in keys]
        except KeyError:
            raise KeyError('CassandraTimeSeries.multiget: no entry exists for item %s at time %s' % (item, datetime64(key[1]).astype(object)))
                

    # todo: does not coerce return dates back to datetime; must change dependencies if u change this
    def selectTimeInterval(self, item, fields, duration, beginTime, endTime, **kw):
        cf = self._getColumnFamily(duration, 'main')
        start = self._makeKey(item, beginTime)
        finish = list(self._makeKey(item, endTime))
        finish.append(False)  # still doesn't exclude the endpoint for some reason, so do it manually
        finish = tuple(finish)
        #print finish
        #print (start, finish, fields)
        results = cf.get_range(start=start, finish=finish, columns=fields)
        results = [result for result in results if result[0][1] <  datetime64(endTime).astype(int)]
        #print results
        return (dict(time=datetime64(x[0][1]).astype(object), **x[1]) for x in results)

    def append(self, item, field, duration, value, time = dtm.utcnow(), **kw):
        #print 'trying to append'
        cf = self._getColumnFamily(duration, 'main')
        cf.insert(self._makeKey(item, time), {field : value})


    def append_interval_observation(self, item, field, duration, begin_time, end_time, status='', source='', confidence=0.0, comment='', **kw):
        #print 'appending %s %s %s %s %s' % (item, field, duration, begin_time, end_time)
        cf = self._getColumnFamily(duration, 'interval_observations')
        begin_time_manual_index_cf = self._getColumnFamily(duration, 'begin_time_manual_index')
        end_time_manual_index_cf = self._getColumnFamily(duration, 'end_time_manual_index')

        interval_columns = {'begin_time': datetime64(begin_time).astype(int), 'end_time': datetime64(end_time).astype(int), field: True, 'status': status, 'source': source, 'confidence': confidence, 'comment': comment}

        #print 'append inserting (%s %s)' % (self._makeIntervalKey(item, begin_time, end_time), interval_columns)
        cf.insert(self._makeIntervalKey(item, begin_time, end_time), interval_columns)
        begin_time_manual_index_cf.insert(self._makeKey(item, begin_time), interval_columns)
        end_time_manual_index_cf.insert(self._makeKey(item, end_time), interval_columns)
        

    #def argsort(iterable):
    #    enumeration = list(enumerate([5,2,3])) 
    #    enumeration.sort(key=lambda (x,y):y)
        


    
    # note: not scalable! could be made somewhat scalable if you used secondary indicies to make a log pyramid grid of containing
    #       reference intervals
    #   also, todo, this whole library should use generators instead of lists
    # todo: does not coerce return dates back to datetime; must change dependencies if u change this
    def overlapping_intervals(self, item, field, duration, begin_time, end_time, confidence_threshold=None, **kw):
            #print 'item, field, duration, begin_time, end_time,  %s %s %s %s %s' % (item, field, duration, begin_time, end_time, )
            cf = self._getColumnFamily(duration, 'interval_observations')
            begin_time_at_or_before_end_time = cf.get_range(finish=(item,datetime64(end_time).astype(int), False), start=(item,))
            #print list(begin_time_at_or_before_end_time)
            begin_time_at_or_before_end_time = cf.get_range(finish=(item,datetime64(end_time).astype(int), False), start=(item,))


            def check_interval_entry(interval_entry, confidence_threshold=None):
                #print interval_entry
                ((item, interval_begin_time, interval_end_time), columns) = interval_entry
                if field in columns and columns[field] and interval_end_time > datetime64(begin_time).astype(int):
                    if confidence_threshold is None or ('confidence' in columns and columns['confidence'] >= confidence_threshold):
                        # note: due to lack of isolation in cassandra you may get an entry with some as-yet unfilled columns...
                        #   todo should use begin_time, end_time in keys, not in columns
                        #   also what is an entry gets in there whose interval has a None on one end? That can't happen via
                        #   .append_inteval_observation but mb someone else mucked up our data
                        #     hack:
                        if ('begin_time' in columns and 'end_time' not in columns) or ('end_time' in columns and 'begin_time' not in columns) or columns['begin_time'] is None or columns['end_time'] is None:
                            logging.debug('CassandraTimeSeries: read an interval observation without a begin_time and end_time: %s.(%s, %s, %s)' (self._keyspace, item, interval_begin_time, interval_end_time))
                            return False
                        return True
                return False

            return (entry for (key, entry) in ifilter(check_interval_entry, begin_time_at_or_before_end_time))
            
    #todo: delete_interval_observation

    # todo return timestamp too
    def earliest_interval_observation_overlapping_interval(self, item, field, duration, begin_time, end_time, confidence_threshold=None):
            observed_rows = list(self.overlapping_intervals(item, field, duration, begin_time, end_time, confidence_threshold=confidence_threshold))
            if not len(observed_rows):
                return None
            observed_beginTimes = [row['begin_time'] for row in observed_rows]
                # note: due to lack of isolation in cassandra you may get an entry with some as-yet unfilled columns...
                #   todo should use begin_time, end_time in keys, not in columns
            result = observed_rows[argmin(observed_beginTimes)]
            return {'begin_time' : datetime64(result['begin_time']).astype(object), 'end_time' : datetime64(result['end_time']).astype(object), 'status' : result['status'], 'source' : result['source'], 'confidence' : result['confidence'], 'comment' : result['comment'], }


    def latest_interval_observation_overlapping_interval(self, item, field, duration, begin_time, end_time, confidence_threshold=None):
            observed_rows = list(self.overlapping_intervals(item, field, duration, begin_time, end_time, confidence_threshold=confidence_threshold))
            if not len(observed_rows):
                return None
            observed_endTimes = [row['end_time'] for row in observed_rows]
            result = observed_rows[argmax(observed_endTimes)]
            return {'begin_time' : datetime64(result['begin_time']).astype(object), 'end_time' : datetime64(result['end_time']).astype(object), 'status' : result['status'], 'source' : result['source'], 'confidence' : result['confidence'], 'comment' : result['comment'], }
            
    def earliest_unobserved_time_within_interval(self, item, field, duration, begin_time, end_time, confidence_threshold=None):
        time = begin_time
        while (time < end_time):
             #print (time, end_time)
             
             next_observed = self.earliest_interval_observation_overlapping_interval(item, field, duration, time, end_time, confidence_threshold=confidence_threshold)
             if next_observed is None or next_observed['begin_time'] > time:
                 return time
             time = next_observed['end_time']
        return None
    
    def latest_unobserved_time_within_interval(self, item, field, duration, begin_time, end_time, confidence_threshold=None):
        time = end_time
        while (begin_time < time):
             prev_observed = self.latest_interval_observation_overlapping_interval(item, field, duration, begin_time, time, confidence_threshold=confidence_threshold)
             if prev_observed is None or prev_observed['end_time'] < time:
                 return time
             time = prev_observed['begin_time']
        return None

    def unobserved_interval_hull_within_interval(self, item, field, duration, begin_time, end_time, confidence_threshold=None):
        #print 'earliest_unobserved: %s, lastest: %s' % (self.earliest_unobserved_time_within_interval(item, field, duration, begin_time, end_time, confidence_threshold), self.latest_unobserved_time_within_interval(item, field, duration, begin_time, end_time, confidence_threshold))
        if begin_time is None and end_time is None:
            return (None, None)
        if (begin_time is None and end_time is not None) or (begin_time is not None and end_time is None) or (end_time < begin_time):
            raise ValueError('CassandraTimeSeries.unobserved_interval_hull_within_interval was passed an invalid interval: (%s, %s)' % (begin_time, end_time))

        return (self.earliest_unobserved_time_within_interval(item, field, duration, begin_time, end_time, confidence_threshold), self.latest_unobserved_time_within_interval(item, field, duration, begin_time, end_time, confidence_threshold))

    @staticmethod
    def interval_union((begin_time1, end_time1), (begin_time2, end_time2)):
        if (begin_time1 is None and end_time1 is not None) or (begin_time1 is not None and end_time1 is None) or (end_time1 < begin_time1):
            raise ValueError('CassandraTimeSeries.interval_union was passed an invalid interval: (%s, %s)' % (begin_time1, end_time1))
        if (begin_time2 is None and end_time2 is not None) or (begin_time2 is not None and end_time2 is None) or (end_time2 < begin_time2):
            raise ValueError('CassandraTimeSeries.interval_union was passed an invalid interval: (%s, %s)' % (begin_time2, end_time2))

        if begin_time1 is None:
            begin_time = begin_time2
        elif begin_time2 is None:
            begin_time = begin_time1
        else:
            begin_time = min(begin_time1, begin_time2)

        if end_time1 is None:
            end_time = end_time2
        elif end_time2 is None:
            end_time = end_time1
        else:
            end_time = max(end_time1, end_time2)

        return (begin_time, end_time)

    # note: requires at least one field, or throws IndexError
    # beware: if you pass a string for argument 'fields' it will just take slices of the string as "field"s
    def unobserved_interval_hull_within_interval_over_fields(self, item, fields, duration, begin_time, end_time, confidence_threshold=None):
        #print 'unobserved_interval_hull_within_interval_over_fields got: %s %s %s %s %s'  % (item, fields, duration, begin_time, end_time)


        interval = self.unobserved_interval_hull_within_interval(item, fields[0], duration, begin_time, end_time, confidence_threshold=confidence_threshold)
        #print 'field %s, interval %s' % (fields[0], interval)

        for field in fields[1:]:
            #print 'field %s, interval %s' % (field, self.unobserved_interval_hull_within_interval(item, field, duration, begin_time, end_time, confidence_threshold=confidence_threshold))
            interval = self.interval_union(interval, self.unobserved_interval_hull_within_interval(item, field, duration, begin_time, end_time, confidence_threshold=confidence_threshold))
        return interval


    def lastEntryInTimeInterval(self, item, field, duration, beginTime, endTime):
        #print (item, [field], duration, beginTime, endTime)
        rows = list(self.selectTimeInterval(item, [field], duration, beginTime, endTime))
        if not len(rows):
            return None
        times = [row['time'] for row in rows]
        result = rows[argmax(times)]
        return result

            

    def firstEntryInTimeInterval(self, item, field, duration, beginTime, endTime):
        #print (item, [field], duration, beginTime, endTime)
        rows = list(self.selectTimeInterval(item, [field], duration, beginTime, endTime))
        if not len(rows):
            return None
        times = [row['time'] for row in rows]
        result = rows[argmin(times)]
        return result
        



    def closestEntryInTime(self, item, field, duration, time, search_radius_duration=timedelta(3,0)):
        try:
            return {'time' : time, 'value' : self.get_nodefault(item, field, time, duration), }
        except KeyError:
            lastBefore = self.lastEntryInTimeInterval(item, field, duration, time - search_radius_duration, time)
            firstAfter = self.firstEntryInTimeInterval(item, field, duration, time, time + search_radius_duration)
            if lastBefore is not None and firstAfter is None:
                return lastBefore

            if firstAfter is not None and lastBefore is None:
                return firstAfter

            if firstAfter is None and lastBefore is None:
                return None

            # only case left: neither are None
            if firstAfter['time'] - time < time - lastBefore['time']:
                return firstAfter
            else:
                return lastBefore 
                

# from cassandratimeseries import *; from datetime import datetime, timedelta; a=CassandraTimeSeries('test'); a.selectTimeInterval('i','f',timedelta(2),datetime.utcnow() - timedelta(60), datetime.utcnow())
# a.append_interval_observation('i','f',timedelta(2),datetime(2012,1,3), datetime(2012,1,5))
# a.append_interval_observation('i','f',timedelta(2),datetime(2012,1,5), datetime(2012,1,8), confidence=1.1)
# a.append_interval_observation('i','f2',timedelta(2),datetime(2012,1,5), datetime(2012,1,8))
# a.append_interval_observation('i','f2',timedelta(2),datetime(2012,1,7), datetime(2012,1,9))


# import cassandratimeseries; reload(cassandratimeseries); from cassandratimeseries import *; a=CassandraTimeSeries('test'); a._pycassa_system_manager.drop_keyspace('test'); a=CassandraTimeSeries('test'); a.append_interval_observation('i','f',timedelta(2),datetime(2012,1,3), datetime(2012,1,5));  a.append_interval_observation('i','f',timedelta(2),datetime(2012,1,5), datetime(2012,1,8), confidence=1.1); a.append_interval_observation('i','f2',timedelta(2),datetime(2012,1,5), datetime(2012,1,8)) ;a.append_interval_observation('i','f2',timedelta(2),datetime(2012,1,7), datetime(2012,1,9)); a.append('i', 'f', timedelta(2), 55, time = datetime(2012,1,6)); a.append('i', 'f', timedelta(2), 60, time = datetime(2012,1,7)); a.append('i', 'f', timedelta(2), 70, time = datetime(2012,1,8));


## list(a.overlapping_intervals('i', 'f', timedelta(2), datetime(2012,1,4), datetime(2012,1,6)))
## Out[132]: 
## [OrderedDict([('begin_time', 1325548800000000), ('comment', u''), ('confidence', 0.0), ('end_time', 1325721600000000), ('f', True), ('source', u''), ('status', u'')]),
##  OrderedDict([('begin_time', 1325721600000000), ('comment', u''), ('confidence', 0.0), ('end_time', 1325980800000000), ('f', True), ('f2', 1.0), ('source', u''), ('status', u'')])]


# type(a.overlapping_intervals('i', 'f', timedelta(2), datetime(2012,1,4), datetime(2012,1,6)))
# Out[131]: generator

# 
# a.earliest_interval_observation_overlapping_interval('i', 'f', timedelta(2), datetime(2012,1,4), datetime(2012,1,6))
## Out[128]: 
## {'begin_time': datetime.datetime(2012, 1, 3, 0, 0),
##  'comment': u'',
##  'confidence': 0.0,
##  'end_time': datetime.datetime(2012, 1, 5, 0, 0),
##  'source': u'',
##  'status': u''}

## a.latest_interval_observation_overlapping_interval('i', 'f', timedelta(2), datetime(2012,1,4), datetime(2012,1,6))Out[127]: 
## {'begin_time': datetime.datetime(2012, 1, 5, 0, 0),
##  'comment': u'',
##  'confidence': 0.0,
##  'end_time': datetime.datetime(2012, 1, 8, 0, 0),
##  'source': u'',
##  'status': u''}


# a.earliest_unobserved_time_within_interval('i', 'f', timedelta(2), datetime(2012,1,5), datetime(2012,1,10))
# Out[123]: datetime.datetime(2012, 1, 8, 0, 0)


# a.earliest_unobserved_time_within_interval('i', 'f', timedelta(2), datetime(2012,1,4), datetime(2012,1,6))
#   returns None

# a.earliest_unobserved_time_within_interval('i', 'f', timedelta(2), datetime(2012,1,2), datetime(2012,1,6))
# Out[125]: datetime.datetime(2012, 1, 2, 0, 0)

# a.latest_unobserved_time_within_interval('i', 'f', timedelta(2), datetime(2012,1,5), datetime(2012,1,10))
# Out[126]: datetime.datetime(2012, 1, 10, 0, 0)



# a.unobserved_interval_hull_within_interval('i', 'f', timedelta(2), datetime(2012,1,5), datetime(2012,1,10))
#Out[119]: (datetime.datetime(2012, 1, 8, 0, 0), datetime.datetime(2012, 1, 10, 0, 0))

# a.unobserved_interval_hull_within_interval('i', 'f2', timedelta(2), datetime(2012,1,5), datetime(2012,1,10))
# Out[120]: (datetime.datetime(2012, 1, 9, 0, 0), datetime.datetime(2012, 1, 10, 0, 0))

# a.unobserved_interval_hull_within_interval_over_fields('i',['f2'],timedelta(2),datetime(2012,1,5), datetime(2012,1,10), confidence_threshold=0)
# Out[121]: (datetime.datetime(2012, 1, 9, 0, 0), datetime.datetime(2012, 1, 10, 0, 0))

# a.unobserved_interval_hull_within_interval_over_fields('i',['f', 'f2'],timedelta(2),datetime(2012,1,5), datetime(2012,1,10), confidence_threshold=0)
# Out[16]: (datetime.datetime(2012, 1, 8, 0, 0), datetime.datetime(2012, 1, 10, 0, 0))
# 


# a.append('i', 'f', timedelta(2), 55, time = datetime(2012,1,6))
# a.append('i', 'f', timedelta(2), 60, time = datetime(2012,1,7))
# a.append('i', 'f', timedelta(2), 70, time = datetime(2012,1,8))

# list(a.selectTimeInterval('i','f',timedelta(2),datetime(2012,1,6), datetime(2012,1,8)))
# Out[181]: 
# [{'time': 1325808000000000, 'value': 55.0},
#  {'time': 1325894400000000, 'value': 60.0}]

# a.closestEntryInTime('i','f',timedelta(2),datetime(2012,1,6,5))
# Out[192]: {'time': datetime.datetime(2012, 1, 6, 0, 0), 'value': 55.0}

# a.closestEntryInTime('i','f',timedelta(2),datetime(2012,1,6,20))
# Out[193]: {'time': datetime.datetime(2012, 1, 7, 0, 0), 'value': 60.0}

# a.selectTimeInterval('i','f',timedelta(2),datetime(2012,1,6), datetime(2012,1,8))
# type(a.selectTimeInterval('i','f',timedelta(2),datetime(2012,1,6), datetime(2012,1,8)))
# Out[160]: generator
