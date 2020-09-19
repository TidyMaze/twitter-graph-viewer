import json

import flask
from flask import jsonify
import networkx as nx
from networkx.readwrite import json_graph
import os

PORT = os.environ.get("PORT")


def get_graph():
    G = nx.barbell_graph(6, 3)
    for n in G:
        G.nodes[n]["name"] = n
    d = json_graph.node_link_data(G)
    return d


# Serve the file over http to allow for cross origin requests
app = flask.Flask(__name__, static_folder="force")


@app.route("/")
def static_proxy():
    return app.send_static_file("force.html")


@app.route("/data")
def get_data():
    return jsonify(get_graph())


print(f"\nGo to http://localhost:{PORT} to see the example\n")
app.run('0.0.0.0', PORT, debug=True)
