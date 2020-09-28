function size() {
    var svg = d3.select("svg")
    var width = document.querySelector('svg').clientWidth
    var height = document.querySelector('svg').clientHeight
    return [width, height]
}

window.onload = function exampleFunction() {
    var svg = d3.select("svg");

    var [width, height] = size()

    console.log(width)
    console.log(height)

    var simulation = d3.forceSimulation()
        .force("link", d3.forceLink().id(function (d) {
            return d.id;
        }).distance(50))
        .force("charge", d3.forceManyBody().strength(-30).distanceMax(100))
        .force("center", d3.forceCenter(width / 2, height / 2))
        .force("collide", d3.forceCollide(15));

    function dragstarted(d) {
        if (!d3.event.active) simulation.alphaTarget(0.3).restart();
        d.fx = d.x;
        d.fy = d.y;
    }

    function dragged(d) {
        d.fx = d3.event.x;
        d.fy = d3.event.y;
    }

    function dragended(d) {
        if (!d3.event.active) simulation.alphaTarget(0);
        d.fx = null;
        d.fy = null;
    }

  // create a tooltip
  var Tooltip = d3.select("#container")
    .append("div")
    .style("opacity", 0)
    .attr("class", "tooltip")
    .style("background-color", "white")
    .style("position", "absolute")
    .style("border", "solid")
    .style("border-width", "1px")
    .style("border-radius", "5px")
    .style("padding", "5px")

  var onclick = function(d) {
    console.log(d)
    Tooltip
      .html(d.kind == 'tweet' ? "Tweet " + d.id : 'Hashtag ' + d.id)
      .style("left", (d3.mouse(this)[0]+70) + "px")
      .style("top", (d3.mouse(this)[1]) + "px")
      .style("opacity", 1)
      .style("border-color", d.kind == 'tweet' ? '#FF9133' : '#87E2F5')

    d3.select(this)
      .attr("stroke-width", "3")
  }

  var onclickoutside = function(d) {
    Tooltip.style("opacity", 0)
  }

    d3.json("/data", function (error, graph) {
        if (error) throw error;

        var link = svg.append("g")
            .attr("class", "links")
            .selectAll("line")
            .data(graph.links)
            .enter().append("line");

        var node = svg.append("g")
            .attr("class", "nodes")
            .selectAll("circle")
            .data(graph.nodes)
            .enter().append("circle")
            .attr("r", 10)

            .classed("tweet", function(el){
              return el.kind == "tweet";
            })
            .classed("hashtag", function(el){
              return el.kind == "hashtag";
            })
            .call(d3.drag()
                .on("start", dragstarted)
                .on("drag", dragged)
                .on("end", dragended));

        node.append("title")
            .text(function (d) {
                return d.id;
            });

        simulation
            .nodes(graph.nodes)
            .on("tick", ticked);

        simulation.force("link")
            .links(graph.links);

        node.on("click", onclick)

        function ticked() {
            link
                .attr("x1", function (d) {
                    return d.source.x;
                })
                .attr("y1", function (d) {
                    return d.source.y;
                })
                .attr("x2", function (d) {
                    return d.target.x;
                })
                .attr("y2", function (d) {
                    return d.target.y;
                });

            node
                .attr("cx", function (d) {
                    return d.x;
                })
                .attr("cy", function (d) {
                    return d.y;
                });
        }
    });
}
