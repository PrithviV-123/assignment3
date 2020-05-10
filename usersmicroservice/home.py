from flask import Flask, request, Response, jsonify
import requests
from configure import db, ip_port, rides_hostname
import re


app = Flask(__name__)



@app.errorhandler(405)
def four_zero_five(e):
	count()
	return Response(status=405)

@app.route('/api/v1/users', methods=["PUT"])
def function_to_add_user():
	count()
	if request.method == "PUT":
		req_d = request.get_json(force=True)

		try:
			u_name = req_d["username"]
			pwd = req_d["password"]
		except KeyError:
			return Response(status=400)

		if re.match(re.compile(r'\b[0-9a-f]{40}\b'), pwd) is None:
			return Response(status=400)

		post_data = {"insert": [u_name, pwd], "columns": ["_id", "password"], "table": "users"}
		resp = requests.post('http://' + ip_port + '/api/v1/db/write', json=post_data)

		if resp.status_code == 400:
			return Response(status=400)

		return Response(status=201, response='{}', mimetype='application/json')

@app.route('/api/v1/users', methods=["GET"])
def function_to_del_user():
	count()
	if request.method == "GET":
		post_data = {"many": 1, "table": "users", "columns": ["_id"], "where": {}}
		resp = requests.post('http://' + ip_port + '/api/v1/db/read', json=post_data)
		res = []
		for i in resp.json():
			res.append(i['_id'])
		if not res:
			return Response(status=204)
		return jsonify(res)
	

@app.route('/api/v1/users/<u_name>', methods=["DELETE"])
def function_to_read_user(u_name):
	count()
	post_data = {'column': '_id', 'delete': u_name, 'table': 'users'}
	resp = requests.post('http://' + ip_port + '/api/v1/db/write', json=post_data)
	if resp.status_code == 400:
		return Response(status=400)
	return jsonify({})

@app.route('/api/v1/_count', methods=["GET"])
def function_to_req_count_get():
	if request.method == "GET":
		f = open("req_cnt.txt", "r")
		res = [int(f.read())]
		f.close()
		return jsonify(res)

@app.route('/api/v1/_count', methods=["DELETE"])
def function_to_req_count_delete():
	if request.method == "DELETE":
		f = open("req_cnt.txt", "w")
		f.write("0")
		f.close()
		return jsonify({})

@app.route('/api/v1/db/write', methods=["POST"])
def function_to_write_db():
	req_d = request.get_json(force=True)

	if 'delete' in req_d:
		try:
			delete = req_d['delete']
			column = req_d['column']
			table = req_d['table']
		except KeyError:
			return Response(status=400)

		try:
			query = {column: delete}
			table = db[table]
			x = table.delete_one(query)
			if x.raw_result['n'] == 1:
				return Response(status=200)
			return Response(status=400)
		except:
			return Response(status=400)

	try:
		insert = req_d['insert']
		columns = req_d['columns']
		table = req_d['table']
	except KeyError:
		return Response(status=400)

	try:
		document = {}
		for i in range(len(columns)):
			document[columns[i]] = insert[i]

		table = db[table]
		table.insert_one(document)
		return Response(status=201)

	except:
		return Response(status=400)

@app.route('/api/v1/db/read', methods=["POST"])
def funtion_to_req_db():
	req_d = request.get_json(force=True)
	try:
		table = req_d['table']
		columns = req_d['columns']
		where = req_d['where']
	except KeyError:
		return Response(status=400)

	filter = {}
	for i in columns:
		filter[i] = 1

	if 'many' in req_d:
		try:
			table = db[table]
			res = []
			for i in table.find(where, filter):
				res.append(i)

			return jsonify(res)
		except:
			return Response(status=400)

	try:
		table = db[table]
		result = table.find_one(where, filter)
		return jsonify(result)
	except:
		return Response(status=400)

@app.route('/api/v1/db/clear', methods=["POST"])
def c_db():
	table1 = db["users"]
	table2 = db["rides"]
	try:
		table1.delete_many({})
		table2.delete_many({})
		f = open("seq.txt", "w")
		f.write("0")
		f.close()
		return Response(status=200)
	except:
		return Response(status=400)

def count():
	filee = open("req_cnt.txt", "r")
	count = int(filee.read())
	filee.close()
	file2 = open("req_cnt.txt", "w")
	file2.write(str(count + 1))
	file2.close()

if __name__ == "__main__":
	app.run(debug=True, host="0.0.0.0", port=80)
