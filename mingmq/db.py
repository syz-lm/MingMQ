from sqlite3 import connect
import logging
import traceback


class AckProcessDB:
    _logger = logging.getLogger('AckProcessDB')
    
    def __init__(self, db_file):
        self._db_file = db_file

        self.create_table()

    def create_table(self):
        conn = None
        c = None
        try:
            conn = connect(self._db_file)
            c = conn.cursor()
            sql = 'create table if not exists ack_msg(' \
                  'message_id varchar(100) primary key, ' \
                  'queue_name text, ' \
                  'message_data text, ' \
                  'pub_date int)'
            c.execute(sql)
        except Exception:
            self._logger.debug(traceback.format_exc())
        finally:
            if c:
                c.close()
            if conn:
                conn.close()

    def insert_message_id_queue_name_message_data_pub_date(
            self,
            message_id,
            queue_name,
            message_data,
            pub_date
    ):
        conn = None
        c = None
        try:
            conn = connect(self._db_file)
            c = conn.cursor()
            sql = 'insert into ack_msg(message_id, queue_name, message_data, pub_date) values(?, ?, ?, ?)'
            args = (message_id, queue_name, message_data, pub_date)
            c.execute(sql, args)
            self._logger.debug('[%s][%s] 影响的行数: %s', repr(sql), repr(args)[:100], repr(c.rowcount))
            conn.commit()
        except Exception:
            if conn: conn.rollback()
            self._logger.debug(traceback.format_exc())
        finally:
            if c:
                c.close()
            if conn:
                conn.close()

    def delete_by_message_id(self, message_id):
        conn = None
        c = None
        try:
            conn = connect(self._db_file)
            c = conn.cursor()
            sql = 'delete from ack_msg where message_id = ?'
            args = (message_id, )
            c.execute(sql, args)
            self._logger.debug('[%s][%s] 影响的行数: %s', repr(sql), repr(args)[:100], repr(c.rowcount))
            conn.commit()
        except Exception:
            if conn: conn.rollback()
            self._logger.debug(traceback.format_exc())
        finally:
            if c:
                c.close()
            if conn:
                conn.close()

    def delete_by_queue_name(self, queue_name):
        conn = None
        c = None
        try:
            conn = connect(self._db_file)
            c = conn.cursor()
            sql = 'delete from ack_msg where queue_name = ?'
            args = (queue_name, )
            c.execute(sql, args)
            self._logger.debug('[%s][%s] 影响的行数: %s', repr(sql), repr(args)[:100], repr(c.rowcount))
            conn.commit()
        except Exception:
            if conn: conn.rollback()
            self._logger.debug(traceback.format_exc())
        finally:
            if c:
                c.close()
            if conn:
                conn.close()

    def pagnation(self, pub_date=None):
        """
        分页获取未确认的任务。
        :param pub_date: 时间戳，若为None则表示不根据时间筛选，反之，则返回小于该时间的的数据。
        :return: list: [message_id, queue_name, message_data, pub_date]
        """
        conn = None
        c = None
        try:
            conn = connect(self._db_file)
            c = conn.cursor()
            sql = 'select message_id, queue_name, message_data, pub_date from ack_msg ' \
                  'order by pub_date desc limit 100'
            args = ()
            if pub_date:
                sql = 'select message_id, queue_name, message_data, pub_date from ack_msg ' \
                      'where pub_date < ? order by pub_date desc limit 100'
                args = (pub_date,)

            c.execute(sql, args)
            result = c.fetchall()
            return result
        except Exception:
            self._logger.debug(traceback.format_exc())
        finally:
            if c:
                c.close()
            if conn:
                conn.close()

    def pagnation_page(self, page):
        """
        分页获取未确认的任务。
        :param pub_date: 时间戳，若为None则表示不根据时间筛选，反之，则返回小于该时间的的数据。
        :return: list: [message_id, queue_name, message_data, pub_date]
        """
        conn = None
        c = None
        try:
            conn = connect(self._db_file)
            c = conn.cursor()
            sql = 'select message_id, queue_name, message_data, pub_date from ack_msg ' \
                  'order by pub_date desc limit ?, 100'
            args = ((page - 1) * 100, )

            c.execute(sql, args)
            result = c.fetchall()
            return result
        except Exception:
            self._logger.debug(traceback.format_exc())
        finally:
            if c:
                c.close()
            if conn:
                conn.close()

    def total_num(self):
        conn = None
        c = None
        try:
            conn = connect(self._db_file)
            c = conn.cursor()
            sql = 'select count(message_id) from ack_msg'
            c.execute(sql, )
            result = c.fetchall()
            return result
        except Exception:
            self._logger.debug(traceback.format_exc())
        finally:
            if c:
                c.close()
            if conn:
                conn.close()


class CompletelyPersistentProcessDB:
    _logger = logging.getLogger('CompletelyPersistentProcessDB')

    def __init__(self, db_file):
        self._db_file = db_file
        self.create_table()

    def create_table(self):
        conn = None
        c = None
        try:
            conn = connect(self._db_file)
            c = conn.cursor()
            sql = 'create table if not exists send_msg(' \
                  'message_id varchar(100) primary key, ' \
                  'queue_name text, ' \
                  'message_data text, ' \
                  'pub_date int)'
            c.execute(sql)
        except Exception:
            self._logger.debug(traceback.format_exc())
        finally:
            if c:
                c.close()
            if conn:
                conn.close()

    def insert_message_id_queue_name_message_data_pub_date(
        self,
        message_id,
        queue_name,
        message_data,
        pub_date
    ):
        conn = None
        c = None
        try:
            conn = connect(self._db_file)
            c = conn.cursor()
            sql = 'insert into send_msg(message_id, queue_name, message_data, pub_date) values(?, ?, ?, ?)'
            args = (message_id, queue_name, message_data, pub_date)
            c.execute(sql, args)
            self._logger.debug('[%s][%s] 影响的行数: %s', repr(sql), repr(args)[:100], repr(c.rowcount))
            conn.commit()
        except Exception:
            if conn: conn.rollback()
            self._logger.debug(traceback.format_exc())
        finally:
            if c:
                c.close()
            if conn:
                conn.close()

    def delete_by_message_id(self, message_id):
        conn = None
        c = None
        try:
            conn = connect(self._db_file)
            c = conn.cursor()
            sql = 'delete from send_msg where message_id = ?'
            args = (message_id, )
            c.execute(sql, args)
            self._logger.debug('[%s][%s] 影响的行数: %s', repr(sql), repr(args)[:100], repr(c.rowcount))
            conn.commit()
        except Exception:
            if conn: conn.rollback()
            self._logger.debug(traceback.format_exc())
        finally:
            if c:
                c.close()
            if conn:
                conn.close()

    def delete_by_queue_name(self, queue_name):
        conn = None
        c = None
        try:
            conn = connect(self._db_file)
            c = conn.cursor()
            sql = 'delete from send_msg where queue_name = ?'
            args = (queue_name, )
            c.execute(sql, args)
            self._logger.debug('[%s][%s] 影响的行数: %s', repr(sql), repr(args)[:100], repr(c.rowcount))
            conn.commit()
        except Exception:
            if conn: conn.rollback()
            self._logger.debug(traceback.format_exc())
        finally:
            if c:
                c.close()
            if conn:
                conn.close()

    def pagnation_page(self, page):
        """
        分页获取未确认的任务。
        :param pub_date: 时间戳，若为None则表示不根据时间筛选，反之，则返回小于该时间的的数据。
        :return: list: [message_id, queue_name, message_data, pub_date]
        """
        conn = None
        c = None
        try:
            conn = connect(self._db_file)
            c = conn.cursor()
            sql = 'select message_id, queue_name, message_data, pub_date from send_msg ' \
                  'order by pub_date asc limit ?, 100'
            args = ((page - 1) * 100, )

            c.execute(sql, args)
            result = c.fetchall()
            return result
        except Exception:
            self._logger.debug(traceback.format_exc())
        finally:
            if c:
                c.close()
            if conn:
                conn.close()

    def total_num(self):
        conn = None
        c = None
        try:
            conn = connect(self._db_file)
            c = conn.cursor()
            sql = 'select count(message_id) from send_msg'
            c.execute(sql, )
            result = c.fetchall()
            return result
        except Exception:
            self._logger.debug(traceback.format_exc())
        finally:
            if c:
                c.close()
            if conn:
                conn.close()