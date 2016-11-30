"""Interface RESTFUL do servidor web."""

from flask import Flask
from flask_restful import Api, Resource
from flask_httpauth import HTTPBasicAuth
from sqlalchemy import *


db = create_engine("mysql+pymysql://danilo:danilo@172.16.128.139:3306/RsiPsd")
db.echo = True
metadata = MetaData(db)


class Query1(Resource):
    def __init__(self):
        """Construtor."""
        super(Query1, self).__init__()

    def get(self):
        connection = db.connect()
        item = connection.execute(
                "select * from `RsiPsd`.`top3Posts` order by `timeChanged` DESC")

        result = item.fetchone()

        connection.close()
        return result.__str__()

class Record(Resource):
    def __init__(self):
        """Construtor."""
        super(Record, self).__init__()

    def get(self, month, year):
        connection = db.connect()
        item = connection.execute(
                "select * from `RsiPsd`.`top3Posts` where `timeChanged` like '{0}-{1}-%%'".format(year, month))

        # result = item.fetchone()
        result = ''
        for i in item:

            result += str(i)+'\n'

        connection.close()
        return result


app = Flask(__name__)
api = Api(app)
auth = HTTPBasicAuth()

api.add_resource(Query1, '/query1/', endpoint='query1')
api.add_resource(Record, '/record/<string:month>/<string:year>/', endpoint='record')


if __name__ == "__main__":
    """Inicia o servidor."""
    host = "0.0.0.0"
    app.run(host, debug=True)
