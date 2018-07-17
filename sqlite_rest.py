#!/usr/bin/env python3
import argparse
import cherrypy
import sqlite3
import enum
import configparser


class AccessType(enum.Enum):
    READ = 1
    WRITE = 2


@cherrypy.expose
class SQLiteREST(object):

    def __init__(self, dbfile, read_allowed=None, write_allowed=None):
        self.db_file = dbfile
        self.read_allowed = read_allowed
        self.write_allowed = write_allowed

    @classmethod
    def to_sqlite_str(cls, val):
        if isinstance(val, str):
            return '"' + val + '"'
        elif val is None:
            return 'NULL'
        return str(val)

    @classmethod
    def normalize_sql_where(cls, query: str):
        semipos = query.rfind(';')
        if semipos >= 0:
            return query[:semipos]
        return query

    def check_access(self, table: str, access: AccessType):
        ref_map = {
            AccessType.READ: self.read_allowed,
            AccessType.WRITE: self.write_allowed
        }
        access_table = ref_map[access]
        if access_table:
            if table not in access_table:
                raise cherrypy.HTTPError(401)

        return True

    @cherrypy.tools.json_out()
    def GET(self, table, query="", describe=False):
        d = []
        self.check_access(table, AccessType.READ)
        with sqlite3.connect(self.db_file) as con:
            con.row_factory = sqlite3.Row
            where = " WHERE " + query if query else ""
            stmt = "SELECT * FROM " + table + where
            try:
                for row in con.execute(stmt):
                    if describe:
                        d = row.keys()
                        break
                    d.append([x for x in row])
            except sqlite3.OperationalError:
                raise cherrypy.HTTPError(404, "Table {t} not found.".format(t=table))
        return d

    @cherrypy.tools.json_in()
    def POST(self, table):
        with sqlite3.connect(self.db_file) as con:
            for row in cherrypy.request.json:
                stmt = "INSERT INTO " + table + " VALUES(" + ",".join([SQLiteREST.to_sqlite_str(x) for x in row]) + ");"
                con.execute(stmt)

    @cherrypy.tools.json_in()
    def PUT(self, table, query=""):
        with sqlite3.connect(self.db_file) as con:
            where = " WHERE " + query if query else ""
            stmt = "UPDATE " + table + " SET " + ",".join(
                [k + '=' + SQLiteREST.to_sqlite_str(v) for k, v in cherrypy.request.json.items()]
            ) + where
            con.execute(stmt)

    @cherrypy.tools.json_in()
    def DELETE(self, table, query=""):
        with sqlite3.connect(self.db_file) as con:
            where = " WHERE " + query if query else ""
            stmt = "DELETE FROM " + table + where
            con.execute(stmt)


def parse_config(config_path, config_):
    def set_config_int(config_, cp, section, key):
        fkey = section + "/" + key
        config_[fkey] = int(cp.get(section, key, fallback=config_[fkey]))

    def set_config(config_, cp, section, key):
        fkey = section + "/" + key
        config_[fkey] = cp.get(section, key, fallback=config_[fkey])

    if config_path:
        cp = configparser.ConfigParser()
        cp.read(config_path)

        if cp.has_section("Network"):
            set_config_int(config_, cp, "Network", "port")

        if cp.has_section("Security"):
            set_config(config_, cp, "Security", "auth")
            set_config(config_, cp, "Security", "user")
            set_config(config_, cp, "Security", "password")

    return config_


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="sqlite REST interface")
    parser.add_argument("-c", "--config", help="configuration file path")
    parser.add_argument("sqlite_file")

    args = parser.parse_args()

    config = {
        "Network/port": 8080,
        "Security/auth": "none",
        "Security/user": "",
        "Security/password": ""
    }

    config = parse_config(args.config, config)

    def validate_password(realm, username, password):
        return username == config['Security/user'] and password == config['Security/password']

    conf = {
        '/': {
            'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
            'tools.response_headers.on': True,
            'tools.auth_basic.on': config['Security/auth'] == 'basic',
            'tools.auth_basic.realm': 'sqlite_secure',
            'tools.auth_basic.checkpassword': validate_password,
            'tools.auth_basic.accept_charset': 'UTF-8'
        },
        'global': {
            'server.socket_port': config["Network/port"]
        }
    }
    cherrypy.quickstart(SQLiteREST(args.sqlite_file), '/', conf)
