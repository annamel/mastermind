from logging import getLogger
from yql.api.v1.client import YqlClient
import time
import datetime


logger = getLogger('mm.planner')

class YqlTableAbsent(Exception):
    pass

class YqlWrapper(object):

    VALIDATE_QUERY = """
        SELECT
          COUNT(*)
        FROM [{table}]
        WHERE source_table="{date_iso}";
    """

    # XXX: maybe something more optimal could be done? How the db is organized?
    # XXX: tounixdate -> arithmetic ops
    # XXX: use HAVING ?
    PREAGGREGATE_QUERY = """
$dateconv = @@
import time
import datetime
def tounixdate(exp_time):
    return int(time.mktime((datetime.date.fromtimestamp(exp_time) + datetime.timedelta(days=1)).timetuple()))
@@;

$dateconvert = Python::tounixdate("(Uint64?)->Uint64", $dateconv);

INSERT INTO [{agr_table}]
SELECT
    couple_id,
    expiration_date,
    namespace,
    "upload" AS operation,
    SUM(CAST(object_size AS Int64)) AS expired_size,
    "{date_iso}" AS source_table,
    {timestamp} AS timestamp
FROM [{main_table}]
WHERE op="upload"
GROUP BY
    couple_id,
    namespace,
    $dateconvert(CAST(expire_at AS Uint64)) AS expiration_date;

INSERT INTO [{agr_table}]
SELECT
    couple_id,
    expiration_date,
    namespace,
    "delete" AS operation,
    -1*SUM(CAST(object_size AS Int64)) AS expired_size,
    "{date_iso}" AS source_table,
    {timestamp} AS timestamp
FROM [{main_table}]
WHERE op="delete" AND expire_at IS NOT NULL
GROUP BY
    couple_id,
    namespace,
    $dateconvert(CAST(expire_at AS Uint64)) AS expiration_date;
        """

    AGGREGATE_QUERY = """
        SELECT couple_id
        FROM
            (SELECT
                couple_id,
                SUM(expired_size) AS sum_expired_size
            FROM [{table}]
            WHERE expiration_date <= {timestamp}
            GROUP BY couple_id)
        WHERE  sum_expired_size >= {trigger};
    """

    REPLACE_QUERY = """
        INSERT INTO [{tmp_table}]
          SELECT * FROM [{base_table}]
          WHERE timestamp >= {starting_time};
        COMMIT;
        DROP TABLE [{base_table}];
        COMMIT;
        INSERT INTO [{base_table}]
          SELECT * FROM [{tmp_table}];
        COMMIT;
        DROP TABLE [{tmp_table}];
    """

    def __init__(self, cluster, token, attempts, delay):
        self._cluster = cluster
        self._token = token
        self._attempts = attempts
        self._delay = delay

    def send_request(self, query, timeout=None):
        """
        Send request to YQL: ping verification + attempts retry
        @return result dict on success, excepts otherwise
        """
        logger.debug('Send YQL request {}'.format(query))
        result = None

        with YqlClient(db=self._cluster, token=self._token) as yql:
            if not yql.ping():
                raise IOError("YQL ping failed {} {}".format(self._cluster, self._token[:20]))

            for attempt in xrange(self._attempts):
                # Do not handle any exceptions here. We do not expect some special event, let it be handled on upper level

                start_time = time.time()

                request = yql.query(query)
                request.run()
                logger.info("YQL query is running with id {}".format(request.operation_id))

                if not request.is_ok:
                    # Most likely there were problem on initial reading of http request.
                    time.sleep(self._delay)
                    # XXXmonitoring
                    logger.error("YQL error: request is not OK on attempt {}. Goto retry".format(attempt))
                    continue

                result = request.results
                end_time = time.time()
                # is_success eq "status in COMPLETED"
                if result.is_success:
                    logger.info("YQL query succeeded and took {}".format(end_time-start_time))
                    if timeout and (end_time - start_time > timeout):
                        # XXXmonitoring: need to add this condition into monitoring
                        # Do not except here - after all we already have some successful result
                        logger.error("YQL error query ({}..{}) exceeds timeout {}".format(start_time, end_time, timeout))
                    return result

                # XXXmonitoring
                logger.error("YQL error {}[{}] ({},{})".format(result.status,
                                request.status_code, request.explain(), str(request.exc_info)))
                logger.error("YQL request was {}".format(request))
                for error in result.errors:
                    if str(error).find("does not exist") != -1:
                        # the table doesn't exist, no sense to retry
                        raise YqlTableAbsent(error)
                    logger.error("YQL query result error {}".format(str(error)))

                # maybe some errors would be better to retry. But for now we just raise exception on any unpredicted status
                raise IOError("YQL unexpected status {}".format(result.status))

        raise IOError("YQL request attempts has exhausted {} {}".format(self._attempts, query))


    def request_expired_stat(self, aggregate_table, expired_threshold, timeout=None):
        """
        Send request to aggregate table to find volume of expired space in couples
        :param aggregate_table: the name of aggregation table
        :param expired_threshold: couples with expired volume above the param would be returned
        :return list of couple ids with expired data more than trigger
        """

        timestamp = int(time.time())
        query = self.AGGREGATE_QUERY.format(table=aggregate_table, timestamp=timestamp, trigger=expired_threshold)

        try:
            r = self.send_request(query, timeout)
            if r.rows_count == 0:
                # it is quite suspicious that we haven't found anything. Maybe wrong table?
                logger.warning("empty result for table {} and trigger {}".format(aggregate_table, expired_threshold))
                return []
        except:
            logger.exception("YT request excepted")
            return []

        valid_couples = []

        for table in r.results:  # access to results blocks until they are ready
            table.fetch_full_data()
            for row in table.rows:
                try:
                    couple = int(row[0])
                except ValueError:
                    logger.exception("couple {} is invalid".format(str(row[0])))
                else:
                    logger.debug("couple {} has more then {} expired bytes".format(couple, expired_threshold))
                    valid_couples.append(couple)

        return valid_couples

    def prepare_aggregate_for_yesterday(self, base_table, aggregate_table):
        """
        Update aggregate table from YT with data from yesterday logs if needed
        :param base_table: YT table with log records filled by mds-proxy
        :param aggregate_table: the name of aggregate table
        """
        yesterday = datetime.date.today() - datetime.timedelta(days=1)

        # check for need to run an aggregation query
        try:
            query = self.VALIDATE_QUERY.format(table=aggregate_table, date_iso=yesterday.isoformat())
            res = self.send_request(query)
            tbl = next(x for x in res.results)
            row = next(x for x in tbl.rows)
            count = int(row[0])
        except YqlTableAbsent:
            count = 0
        except:
            logger.exception("Analysis of validation results has excepted")
            raise

        if not count:
            self.prepare_aggregate_table(base_table, aggregate_table, yesterday)
        else:
            logger.info("Skip write into aggregation table due to {} records from {}".format(count, str(yesterday)))

    def prepare_aggregate_table(self, base_table, aggregate_table, date):
        """
        Update aggregate table from base_table/date
        :param base_table: YT table with log records filled by mds-proxy
        :param aggregate_table: the name of aggregate_table (where to store data)
        :param date: date for which we extract log records that would be added to aggregate_tbale
        """
        date_iso = date.isoformat()
        main_table_per_day = "{}/{}".format(base_table, date_iso)

        query = self.PREAGGREGATE_QUERY.format(agr_table=aggregate_table, main_table=main_table_per_day,
                                               timestamp=int(time.time()), date_iso=date_iso)

        return self.send_request(query)

    def cleanup_aggregate_table(self, aggregate_table, couples_hist):
        """
        Aggregate table is to be cleaned up from time to time in order to speed up aggregate_query
        We could remove records that have expiration_date less then the last ttl_cleanup run on this couple
        But since there is no REMOVE request in YQL we need to create a new table without outdated records
        :param aggregate_table: a name of aggregate table
        :param couples_hist: dict[ 'couple_id' ] = last_run
        :exception IOError (from YQL operations), ValueError
        """

        tmp_table_name = aggregate_table + "_tmp"

        # Read the entire table content
        query = "SELECT * FROM [{}];".format(aggregate_table)
        r = self.send_request(query)

        try:
            table = next(x for x in r.results)
        except:
            logger.exception("Incorrect results of query {}".format(query))

        records_to_remove = 0

        table.fetch_full_data()
        starting_time = time.time()

        ins_query = "INSERT INTO [" + tmp_table_name + "] (" + ", ".join(cln[0] for cln in table.columns) + ") VALUES "

        couple_repr = ('couple_id', 'String')
        exp_repr = ('expiration_date', 'Uint64')

        if couple_repr not in table.columns or exp_repr not in table.columns:
            raise ValueError("Incorrect table columns {}".format(table.columns))
        couple_idx = table.columns.index(couple_repr)
        exp_idx = table.columns.index(exp_repr)

        # do not create a new dict for the sake of memory. do not modify table.row for the sake of performance
        # instead create insertion request just in place
        for row in table.rows:
            couple_id = int(row[couple_idx])
            expiration = int(row[exp_idx])

            if couple_id not in couples_hist:
                logger.error("couple {} not in couples_hist {}".format(couple_id, couples_hist))
                continue

            if couples_hist[couple_id] >= expiration:
                continue

            converting_func = lambda r, i: "CAST({} AS {})".format(r, table.columns[i][1]) \
                if table.columns[i][1] != "String" else ("'" + str(r) + "'")

            ins_query += "(" + ", ".join(converting_func(r, row.index(r)) for r in row) + "),"
            records_to_remove += 1

        ins_query = ins_query[:-1] + ";"

        if records_to_remove == 0:
            # no need to except: maybe cleanup of aggregate table run frequently due to test reasons
            # but anyway we'd better to notify of that
            logger.warn("No outdated records in aggregate table were found (couples_hist {})".format(len(couples_hist)))
            return

        # We can drop old aggregate table and insert the new one at once (one request is protected by trans lock)
        # But delivering values from outside could take much longer then transferring values from one table into another
        # So it's preferrable to prepare a temp table before
        self.send_request(ins_query)

        # We need to perform the following sequence:
        #  - drop old aggregate table
        #  - transfer data from tmp table into aggregate table
        #  - drop tmp table
        # All those actions should be performed within one YQL operation to guarantee lock for the table.
        # But in the beginning we should get update from aggregate table
        # (i.e. values that were recorded during our processing and thus that are absent in tmp_table)
        replace_query = self.REPLACE_QUERY.format(
            starting_time=starting_time, tmp_table=tmp_table_name, base_table=aggregate_table)

        self.send_request(replace_query)
