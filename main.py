from sodapy import Socrata
import rethinkdb as r


def fetch_data():
	# Make a connection	
	conn = r.connect(host="localhost", port=28015, db="test")
	# You need to register the App Token (manually?)
	client = Socrata("data.sunshinecoast.qld.gov.au", "6MbT9NoWolynKM1ooRzrvm7Fs")
	# API Endpoint
	endpoint = "/resource/mn3m-fqri.json"
	off = 0
	while True:
		data = client.get(endpoint, limit=50000,offset=off)
		if len(data) == 0:
			break
		for elem in data:
			try:
				# Only store if we have a date
				cur = elem['d_date_rec']
				r.table("planning").insert(elem).run(conn)
			except KeyError:
				pass
		off = off+50000



fetch_data()
