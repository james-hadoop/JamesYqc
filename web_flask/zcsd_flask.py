#!flask/bin/python
# coding=utf-8
import json

from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow

# # 跨域支持
# def after_request(resp):
#     resp.headers['Access-Control-Allow-Origin'] = '*'
#     resp.headers['Access-Control-Allow-Headers'] = 'Content-Type'
#     resp.headers['Access-Control-Allow-Methods'] = 'GET,PUT,POST,DELETE'
#
#     return resp
from sqlalchemy import inspect

app = Flask(__name__)

# app.after_request(after_request)

HOSTNAME = '127.0.0.1'
PORT = '3306'
DATABASE = 'developer'
USERNAME = 'developer'
PASSWORD = 'developer'

# dialect+driver://username:password@host:port/database
DB_URI = "mysql+pymysql://{username}:{password}@{host}:{port}/{db}?charset=UTF8MB4".format(username=USERNAME,
                                                                                           password=PASSWORD,
                                                                                           host=HOSTNAME, port=PORT,
                                                                                           db=DATABASE)

app.config['SQLALCHEMY_DATABASE_URI'] = DB_URI
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
ma = Marshmallow(app)


class Serializer(object):

    def serialize(self):
        return {c: getattr(self, c) for c in inspect(self).attrs.keys()}

    @staticmethod
    def serialize_list(l):
        return [m.serialize() for m in l]


class UserLogSummary(db.Model):
    __tablename__ = 't_dwa_zcsd_user_log_summary'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    uid = db.Column(db.String(50), nullable=False)
    cid = db.Column(db.String(50), nullable=False)
    pid = db.Column(db.String(50), nullable=False)
    ts = db.Column(db.DateTime, nullable=False)
    ts_str = db.Column(db.String(50), nullable=False)
    lat_str = db.Column(db.String(50), nullable=True)
    lon_str = db.Column(db.String(50), nullable=True)
    op = db.Column(db.String(50), nullable=False)
    cnt = db.Column(db.Integer, nullable=False)
    nickname = db.Column(db.String(50), nullable=True)
    phone = db.Column(db.String(50), nullable=True)
    age = db.Column(db.String(50), nullable=True)
    sex = db.Column(db.String(50), nullable=True)
    company = db.Column(db.String(200), nullable=True)
    duty = db.Column(db.String(200), nullable=True)
    title = db.Column(db.String(200), nullable=True)
    label_industry_ids = db.Column(db.String(200), nullable=True)
    label_place_ids = db.Column(db.String(200), nullable=True)
    location = db.Column(db.String(200), nullable=True)

    def __repr__(self):
        ret = '{"id"="%s", "nickname"="%s", "phone"="%s", "uid"="%s","cid"="%s", "ts_str"="%s", "lat_str"="%s", "lon_str"="%s", "op"="%s", "cnt"="%s"}' % (
            self.id, self.nickname, self.phone, self.uid, self.cid, self.ts_str, self.lat_str, self.lat_str,
            self.op,
            self.cnt)

        return ret

    # def toJSON(self):
    #     return json.dumps(self, default=lambda o: o.__dict__,
    #                       sort_keys=True, indent=4)
    #
    # def serialize(self):
    #     d = Serializer.serialize(self)
    #     return d


# class UserLogSummarySchema(ma.SQLAlchemySchema):
#     class Meta:
#         model = UserLogSummary
#         dump_only = "id"
#         load_instance = True
#         include_fk = True


# curl http://localhost:18001/ping
@app.route('/ping', methods=['GET', 'POST'])
def ping():
    return dict(code=200, data="pong", size=1)


# curl http://localhost:18001/get_all
@app.route('/get_all', methods=['GET', 'POST'])
def get_all():
    items = UserLogSummary.query.all()


    return jsonify(json_list = items)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=18001)
