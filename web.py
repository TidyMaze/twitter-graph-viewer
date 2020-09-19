import json

import flask
from flask import jsonify
import networkx as nx
from networkx.readwrite import json_graph
import os
from twitter_repository import *

PORT = os.environ.get("PORT")


def get_graph():
    G = nx.barbell_graph(6, 3)
    for n in G:
        G.nodes[n]["name"] = n
    return json_graph.node_link_data(G)

def result_as_graph(neo4j_result):
    G=nx.Graph()
    print(neo4j_result)
    for row in neo4j_result:
        print(f'row: {row}')
        G.add_edge(row['r'][0]['id'], row['r'][2]['tag'])
    return json_graph.node_link_data(G)


# Serve the file over http to allow for cross origin requests
app = flask.Flask(__name__, static_folder="force")


@app.route("/")
def static_proxy():
    return app.send_static_file("force.html")

@app.route("/data")
def get_data():
    with get_neo4j_driver() as driver:
        records = get_all(driver)
        return jsonify(result_as_graph(records))


print(f"\nGo to http://localhost:{PORT} to see the example\n")
app.run('0.0.0.0', PORT, debug=True)
