from flask import Flask, request, Response, jsonify
from datetime import datetime
from configure import db, areas, ip_port, load_balancer, rides_dns_name
import requests

app = Flask(__name__)


@app.errorhandler(405)
def four_zero_five(e):
	count()
	return Response(status=405)


@app.route('/api/v1/rides', methods=["POST"])
def function_to_create_ride():
	count()
	req_data = request.get_json(force=True)
	try:
		created_by = req_data['created_by']
		time_stamp = req_data['timestamp']
		src = int(req_data['source'])
		dst = int(req_data['destination'])
	except KeyError:
		return Response(status=400)

	try:
		req_date = convert_timestamp_to_datetime(time_stamp)
	except:
		return Response(status=400)

	if (src > len(areas) or dst > len(areas)) and (src < 1 or dst < 1):
		return Response(status=400)
   # if not isUserPresent(created_by):
    #    print("User not present")
     #   return Response(status=400)

	try:
		f = open('seq.txt', 'r')
		r_count = int(f.read())
		f.close()
		binded_data = {
            "insert": [r_count + 1, r_count + 1, created_by, time_stamp, areas[src-1][1], areas[dst-1][1], []],
            "columns": ["_id", "rideId", "created_by", "timestamp", "source", "destination", "users"], "table": "rides"}
		resp = requests.post('http://' + ip_port + '/api/v1/db/write', json=binded_data)
		if resp.status_code == 400:
			return Response(status=400)
		else:
			f = open('seq.txt', 'w')
			f.write(str(r_count + 1))
			f.close()
			return Response(status=201, response='{}', mimetype='application/json')
	except:
		return Response(status=400)


@app.route('/api/v1/rides', methods=["GET"])
def list_rides_between_src_and_dst():
	count()
	source = request.args.get("source")
	destination = request.args.get("destination")
	if source is None or destination is None:
		return Response(status=400)

	try:
		source = int(source)
		destination = int(destination)
	except:
		return Response(status=400)

	if (source > len(areas) or destination > len(areas)) and (source < 1 or destination < 1):
		return Response(status=400)

	post_data = {"many": 1, "table": "rides", "columns": ["rideId", "created_by", "timestamp"],
                 "where": {"source": areas[source-1][1], "destination": areas[destination-1][1], "timestamp": {"$gt": convert_datetime_to_timestamp(datetime.now())}}}
	response = requests.post('http://' + ip_port + '/api/v1/db/read', json=post_data)

	if response.status_code == 400:
		return Response(status=400)

	result = response.json()
	for i in range(len(result)):
		if "_id" in result[i]:
			del result[i]["_id"]

	if not result:
		return Response(status=204)
	return jsonify(result)
@app.route('/api/v1/rides/<rideId>', methods=["GET"])
def details_of_ride(rideId):
	count()
	try:
		a = int(rideId)
	except:
		return Response(status=400)

	if request.method == "GET":
		binded_data = {"table": "rides",
                     "columns": ["rideId", "created_by", "users", "timestamp", "source", "destination"],
                     "where": {"rideId": int(rideId)}}
		response = requests.post('http://' + ip_port + '/api/v1/db/read', json=binded_data)
		if response.text == "":
			return Response(status=204, response='{}', mimetype='application/json')
		res = response.json()
		del res["_id"]
		return jsonify(res)

@app.route('/api/v1/rides/<rideId>', methods=["POST"])
def details_of_join_ride(rideId):
	count()
	try:
		a = int(rideId)
	except:
		return Response(status=400)
	if request.method == "POST":
		username = request.get_json(force=True)["username"]
     #   if not isUserPresent(username):
            # print("User not present")
      #      return Response(status=400)

		binded_data = {"table": "rides", "where": {"rideId": int(rideId)}, "update": "users", "data": username,
                     "operation": "addToSet"}
		response = requests.post('http://' + ip_port + '/api/v1/db/write', json=binded_data)
		if response.status_code == 400:
			return Response(status=400)
		return jsonify({})

@app.route('/api/v1/rides/<rideId>', methods=["DELETE"])
def details_of_delete_ride(rideId):
	count()
	try:
		a = int(rideId)
	except:
		return Response(status=400)
	if request.method == "DELETE":
		binded_data = {'column': 'rideId', 'delete': int(rideId), 'table': 'rides'}
		response = requests.post('http://' + ip_port + '/api/v1/db/write', json=binded_data)
		if response.status_code == 400:
			return Response(status=400)
		return jsonify({})


@app.route('/api/v1/rides/count', methods=["GET"])
def function_to_view_no_of_rides():
	count()
	binded_data = {"count": 1, "table": "rides"}
	response = requests.post('http://' + ip_port + '/api/v1/db/read', json=binded_data)
	return jsonify(response.json())


@app.route('/api/v1/_count', methods=["GET"])
def req_count_get():
	if request.method == "GET":
		f = open("req_count.txt", "r")
		res = [int(f.read())]
		f.close()
		return jsonify(res)

@app.route('/api/v1/_count', methods=["DELETE"])
def req_count_delete():
	if request.method == "DELETE":
		f = open("req_count.txt", "w")
		f.write("0")
		f.close()
		return jsonify({})

@app.route('/api/v1/db/write', methods=["POST"])
def write_to_db():
	req_data = request.get_json(force=True)
	if 'update' in req_data:
		try:
			collection = req_data['table']
			where = req_data['where']
			array = req_data['update']
			data = req_data['data']
			operation = req_data['operation']
		except KeyError:
			return Response(status=400)
		try:
			collection = db[collection]
			x = collection.update_one(where, {"$" + operation: {array: data}})
			if x.raw_result['n'] == 1:
				return Response(status=200)
			return Response(status=400)
		except:
			return Response(status=400)

	try:
		insert = req_data['insert']
		columns = req_data['columns']
		collection = req_data['table']
	except KeyError:
		return Response(status=400)

	try:
		document = {}
		for i in range(len(columns)):
			if columns[i] == "timestamp":
				document[columns[i]] = convert_timestamp_to_datetime(insert[i])
			else:
				document[columns[i]] = insert[i]

		collection = db[collection]
		collection.insert_one(document)
		return Response(status=201)

	except:
		return Response(status=400)


@app.route('/api/v1/db/write', methods=["POST"])
def write_to_db_delete():
	req_data = request.get_json(force=True)

	if 'delete' in req_data:
		try:
			delete = req_data['delete']
			column = req_data['column']
			collection = req_data['table']
		except KeyError:
			return Response(status=400)

		try:
			query = {column: delete}
			collection = db[collection]
			x = collection.delete_one(query)
			if x.raw_result['n'] == 1:
				return Response(status=200)
			return Response(status=400)
		except:
			return Response(status=400)


@app.route('/api/v1/db/read', methods=["POST"])
def read_from_db():
	req_data = request.get_json(force=True)

	if 'count' in req_data:
		try:
			collection = db[req_data['table']]
			res = [collection.count_documents({})]
			return jsonify(res)
		except:
			return Response(status=400)

	try:
		table = req_data['table']
		columns = req_data['columns']
		where = req_data['where']
	except KeyError:
		return Response(status=400)

	if "timestamp" in where:
		where["timestamp"]["$gt"] = convert_timestamp_to_datetime(where["timestamp"]["$gt"])

	filter = {}
	for i in columns:
		filter[i] = 1

	if 'many' in req_data:
		try:
			collection = db[table]
			res = []
			for i in collection.find(where, filter):
				if "timestamp" in i:
					i["timestamp"] = convert_datetime_to_timestamp(i["timestamp"])
				res.append(i)

			return jsonify(res)
		except:
			return Response(status=400)

	try:
		collection = db[table]
		result = collection.find_one(where, filter)
		if "timestamp" in result:
			result["timestamp"] = convert_datetime_to_timestamp(result["timestamp"])
		return jsonify(result)
	except:
		return Response(status=400)


@app.route('/api/v1/db/clear', methods=["POST"])
def clear_db():
	collection1 = db["users"]
	collection2 = db["rides"]
	try:
		collection1.delete_many({})
		collection2.delete_many({})
		f = open("seq.txt", "w")
		f.write("0")
		f.close()
		return Response(status=200)
	except:
		return Response(status=400)


#def isUserPresent(username):
 #   response = requests.get('http://' + load_balancer + '/api/v1/users', headers={"Origin": rides_dns_name})
  #  return response.status_code != 400 and username in response.json()


def convert_datetime_to_timestamp(k):
	day = str(k.day) if len(str(k.day)) == 2 else "0" + str(k.day)
	month = str(k.month) if len(str(k.month)) == 2 else "0" + str(k.month)
	year = str(k.year)
	second = str(k.second) if len(str(k.second)) == 2 else "0" + str(k.second)
	minute = str(k.minute) if len(str(k.minute)) == 2 else "0" + str(k.minute)
	hour = str(k.hour) if len(str(k.hour)) == 2 else "0" + str(k.hour)
	return day + "-" + month + "-" + year + ":" + second + "-" + minute + "-" + hour


def convert_timestamp_to_datetime(time_stamp):
	day = int(time_stamp[0:2])
	month = int(time_stamp[3:5])
	year = int(time_stamp[6:10])
	seconds = int(time_stamp[11:13])
	minutes = int(time_stamp[14:16])
	hours = int(time_stamp[17:19])
	return datetime(year, month, day, hours, minutes, seconds)


def count():
	f = open("req_count.txt", "r")
	count = int(f.read())
	f.close()
	f2 = open("req_count.txt", "w")
	f2.write(str(count + 1))
	f2.close()


if __name__ == "__main__":
	app.run(debug=True, host="0.0.0.0", port=80)
