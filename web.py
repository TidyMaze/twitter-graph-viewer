import json

import flask
import networkx as nx
from networkx.readwrite import json_graph
import os

PORT = os.environ.get("PORT")

G = nx.barbell_graph(6, 3)
# this d3 example uses the name attribute for the mouse-hover value,
# so add a name to each node
for n in G:
    G.nodes[n]["name"] = n
# write json formatted data
d = json_graph.node_link_data(G)  # node-link format to serialize
# write json
json.dump(d, open("force/force.json", "w"))
print("Wrote node-link JSON data to force/force.json")

# Serve the file over http to allow for cross origin requests
app = flask.Flask(__name__, static_folder="force")


@app.route("/")
def static_proxy():
    return app.send_static_file("force.html")


print(f"\nGo to http://localhost:{PORT} to see the example\n")
app.run('0.0.0.0', PORT)
