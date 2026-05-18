var _svg, _g, _zoom, _svgW, _svgH;
var _nodePositions = {};
var _edgePaths = [];
var _dagreGraph = null;
var _selectedNode = null;
var _scopedNodes = null;

var TYPE_COLORS = {
    dbt_source:'#6b7280', dbt_model:'#9ca3af', table:'#3b82f6', metric:'#10b981',
    relationship:'#f59e0b', semantic_view:'#8b5cf6', filter:'#ef4444',
    verified_query:'#06b6d4', custom_instruction:'#6b7280'
};
var HIDDEN_TYPES = {};

function renderLineage() {
    var container = document.getElementById('lineage-container');
    if (!container || !GRAPH_DATA || !GRAPH_DATA.nodes || !GRAPH_DATA.nodes.length) return;

    _svgW = container.clientWidth;
    _svgH = container.clientHeight;

    var g = new dagre.graphlib.Graph();
    g.setGraph({rankdir:'LR', ranksep:80, nodesep:30, edgesep:15, marginx:40, marginy:40});
    g.setDefaultEdgeLabel(function() { return {}; });

    GRAPH_DATA.nodes.forEach(function(n) {
        var w = Math.max(n.name.length * 7 + 24, 100);
        g.setNode(n.id, {label:n.name, width:w, height:32, type:n.type, layer:n.layer||0, metadata:n.metadata});
    });
    GRAPH_DATA.links.forEach(function(l) {
        var src = typeof l.source === 'object' ? l.source.id : l.source;
        var tgt = typeof l.target === 'object' ? l.target.id : l.target;
        if (g.hasNode(src) && g.hasNode(tgt)) {
            g.setEdge(tgt, src, {type:l.type});
        }
    });
    dagre.layout(g);
    _dagreGraph = g;

    g.nodes().forEach(function(nid) {
        var node = g.node(nid);
        _nodePositions[nid] = {x: node.x, y: node.y, w: node.width, h: node.height};
    });

    _svg = d3.select('#lineage-container').append('svg').attr('width', _svgW).attr('height', _svgH).attr('class','lineage-svg');

    var defs = _svg.append('defs');
    defs.append('marker').attr('id','arrow').attr('viewBox','0 -4 8 8').attr('refX',8).attr('refY',0)
        .attr('markerWidth',6).attr('markerHeight',6).attr('orient','auto')
        .append('path').attr('d','M0,-4L8,0L0,4').attr('fill','#94a3b8');

    _g = _svg.append('g');
    _zoom = d3.zoom().scaleExtent([0.1,3]).on('zoom', function(e) { _g.attr('transform', e.transform); });
    _svg.call(_zoom);

    _edgePaths = [];
    g.edges().forEach(function(e) {
        var edge = g.edge(e);
        var isDashed = edge.type === 'includes' || edge.type === 'uses';
        var path = _g.append('path')
            .attr('class', 'lineage-edge' + (isDashed ? ' dashed' : ''))
            .attr('data-source', e.v).attr('data-target', e.w);
        _edgePaths.push({path: path, v: e.v, w: e.w});
    });
    _updateAllEdges();

    var drag = d3.drag()
        .on('start', function(event) {
            event.sourceEvent.stopPropagation();
            d3.select(this).raise().classed('dragging', true);
        })
        .on('drag', function(event) {
            var nid = this.dataset.nodeId;
            var pos = _nodePositions[nid];
            if (!pos) return;
            pos.x += event.dx;
            pos.y += event.dy;
            d3.select(this).attr('transform', 'translate(' + (pos.x - pos.w/2) + ',' + (pos.y - pos.h/2) + ')');
            _updateEdgesForNode(nid);
        })
        .on('end', function() {
            d3.select(this).classed('dragging', false);
        });

    g.nodes().forEach(function(nid) {
        var node = g.node(nid);
        var color = TYPE_COLORS[node.type] || '#6b7280';
        var group = _g.append('g').attr('class','lineage-node').attr('data-type', node.type)
            .attr('data-node-id', nid)
            .attr('transform', 'translate(' + (node.x - node.width/2) + ',' + (node.y - node.height/2) + ')')
            .style('cursor','grab')
            .on('click', function(event) {
                if (event.defaultPrevented) return;
                _highlightNode(nid);
                var name = GRAPH_DATA.nodes.find(function(n){return n.id===nid});
                if (name) showDetail(nid.split(':')[0], name.name);
            })
            .call(drag);
        group.append('rect').attr('width', node.width).attr('height', node.height)
            .attr('rx',4).attr('ry',4).attr('fill', color).attr('opacity', 0.12)
            .attr('stroke', color).attr('stroke-width', 1.5);
        group.append('text')
            .attr('x', node.width/2).attr('y', node.height/2 + 4)
            .attr('text-anchor','middle').attr('font-size','11px')
            .attr('fill', 'var(--text-primary)')
            .text(node.label.length > 20 ? node.label.substring(0,18) + '..' : node.label);
    });

    buildToolbar();
    zoomFit();
}

function _edgeLine(srcPos, tgtPos) {
    var sx = srcPos.x + srcPos.w/2;
    var sy = srcPos.y;
    var tx = tgtPos.x - tgtPos.w/2;
    var ty = tgtPos.y;
    var mx = (sx + tx) / 2;
    return 'M' + sx + ',' + sy + 'C' + mx + ',' + sy + ' ' + mx + ',' + ty + ' ' + tx + ',' + ty;
}

function _updateAllEdges() {
    _edgePaths.forEach(function(ep) {
        var sp = _nodePositions[ep.v];
        var tp = _nodePositions[ep.w];
        if (sp && tp) ep.path.attr('d', _edgeLine(sp, tp));
    });
}

function _updateEdgesForNode(nid) {
    _edgePaths.forEach(function(ep) {
        if (ep.v === nid || ep.w === nid) {
            var sp = _nodePositions[ep.v];
            var tp = _nodePositions[ep.w];
            if (sp && tp) ep.path.attr('d', _edgeLine(sp, tp));
        }
    });
}

function buildToolbar() {
    var types = {};
    GRAPH_DATA.nodes.forEach(function(n) { types[n.type] = (types[n.type]||0) + 1; });
    var toolbar = document.getElementById('lineage-toolbar');
    toolbar.innerHTML = '';
    Object.keys(types).sort().forEach(function(t) {
        var label = document.createElement('label');
        var cb = document.createElement('input');
        cb.type = 'checkbox'; cb.checked = true; cb.value = t;
        cb.addEventListener('change', function() {
            if (this.checked) delete HIDDEN_TYPES[t]; else HIDDEN_TYPES[t] = true;
            applyFilters();
        });
        label.appendChild(cb);
        label.appendChild(document.createTextNode(' ' + t.replace('_',' ')));
        toolbar.appendChild(label);
    });
}

function applyFilters() {
    _g.selectAll('.lineage-node').style('display', function() {
        var nid = this.dataset.nodeId;
        if (HIDDEN_TYPES[this.dataset.type]) return 'none';
        if (_scopedNodes && !_scopedNodes[nid]) return 'none';
        return null;
    });
    _g.selectAll('.lineage-edge').style('display', function() {
        var src = this.dataset.source;
        var tgt = this.dataset.target;
        var srcNode = GRAPH_DATA.nodes.find(function(n){return n.id===src});
        var tgtNode = GRAPH_DATA.nodes.find(function(n){return n.id===tgt});
        if (srcNode && HIDDEN_TYPES[srcNode.type]) return 'none';
        if (tgtNode && HIDDEN_TYPES[tgtNode.type]) return 'none';
        if (_scopedNodes && (!_scopedNodes[src] || !_scopedNodes[tgt])) return 'none';
        return null;
    });
}

function scopeToNodes(nodeSet) {
    _scopedNodes = nodeSet;
    _clearHighlight();
    applyFilters();
    setTimeout(zoomFit, 50);
}

function clearScope() {
    _scopedNodes = null;
    _clearHighlight();
    applyFilters();
    setTimeout(zoomFit, 50);
}

function zoomIn() { _svg.transition().duration(300).call(_zoom.scaleBy, 1.4); }
function zoomOut() { _svg.transition().duration(300).call(_zoom.scaleBy, 0.7); }
function zoomFit() {
    if (!_g || !_svg) return;
    var bounds = _g.node().getBBox();
    if (bounds.width === 0) return;
    var scale = Math.min(_svgW / (bounds.width + 80), _svgH / (bounds.height + 80), 1);
    var tx = (_svgW - bounds.width * scale) / 2 - bounds.x * scale;
    var ty = (_svgH - bounds.height * scale) / 2 - bounds.y * scale;
    _svg.transition().duration(500).call(_zoom.transform, d3.zoomIdentity.translate(tx, ty).scale(scale));
}

function _highlightNode(nid) {
    if (!_g) return;
    if (_selectedNode === nid) {
        _clearHighlight();
        return;
    }
    _selectedNode = nid;

    var connected = {};
    connected[nid] = true;
    _edgePaths.forEach(function(ep) {
        if (ep.v === nid) connected[ep.w] = true;
        if (ep.w === nid) connected[ep.v] = true;
    });

    _g.selectAll('.lineage-node').each(function() {
        var id = this.dataset.nodeId;
        var sel = d3.select(this);
        if (id === nid) {
            sel.classed('selected', true).classed('dimmed', false);
            sel.select('rect').attr('stroke-width', 2.5).attr('opacity', 0.25);
        } else if (connected[id]) {
            sel.classed('selected', false).classed('dimmed', false);
            sel.select('rect').attr('stroke-width', 1.5).attr('opacity', 0.12);
        } else {
            sel.classed('selected', false).classed('dimmed', true);
            sel.select('rect').attr('stroke-width', 1).attr('opacity', 0.05);
        }
    });

    _edgePaths.forEach(function(ep) {
        var isConnected = ep.v === nid || ep.w === nid;
        ep.path.classed('edge-highlighted', isConnected).classed('edge-dimmed', !isConnected);
    });
}

function _clearHighlight() {
    _selectedNode = null;
    if (!_g) return;
    _g.selectAll('.lineage-node').each(function() {
        var sel = d3.select(this);
        sel.classed('selected', false).classed('dimmed', false);
        var nid = this.dataset.nodeId;
        var nodeData = GRAPH_DATA.nodes.find(function(n){return n.id===nid});
        var color = nodeData ? (TYPE_COLORS[nodeData.type] || '#6b7280') : '#6b7280';
        sel.select('rect').attr('stroke-width', 1.5).attr('opacity', 0.12).attr('stroke', color);
    });
    _edgePaths.forEach(function(ep) {
        ep.path.classed('edge-highlighted', false).classed('edge-dimmed', false);
    });
}

function focusLineageNode(nodeId) {
    if (!_g) return;
    _highlightNode(nodeId);
    var pos = _nodePositions[nodeId];
    if (!pos) return;
    var scale = 1.2;
    var tx = _svgW/2 - pos.x * scale;
    var ty = _svgH/2 - pos.y * scale;
    _svg.transition().duration(500).call(_zoom.transform, d3.zoomIdentity.translate(tx, ty).scale(scale));
}
