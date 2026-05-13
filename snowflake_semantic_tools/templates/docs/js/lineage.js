function initLineage(graphData) {
    var container = document.getElementById('lineage-graph');
    if (!container || !graphData || !graphData.nodes) return;

    var width = container.clientWidth;
    var height = container.clientHeight;

    var colorMap = {
        table: '#4a90d9', metric: '#27ae60', relationship: '#f39c12',
        semantic_view: '#8e44ad', filter: '#e74c3c',
        custom_instruction: '#95a5a6', verified_query: '#16a085'
    };
    var radiusMap = {
        table: 20, metric: 16, relationship: 14,
        semantic_view: 22, filter: 14,
        custom_instruction: 12, verified_query: 14
    };

    var svg = d3.select('#lineage-graph')
        .append('svg')
        .attr('width', width)
        .attr('height', height)
        .attr('class', 'sst-lineage-svg');

    var defs = svg.append('defs');
    defs.append('marker')
        .attr('id', 'arrowhead')
        .attr('viewBox', '0 -5 10 10')
        .attr('refX', 28)
        .attr('refY', 0)
        .attr('markerWidth', 6)
        .attr('markerHeight', 6)
        .attr('orient', 'auto')
        .append('path')
        .attr('d', 'M0,-5L10,0L0,5')
        .attr('fill', '#999');

    var g = svg.append('g');

    var zoom = d3.zoom()
        .scaleExtent([0.1, 4])
        .on('zoom', function(event) { g.attr('transform', event.transform); });
    svg.call(zoom);

    var nodes = graphData.nodes.map(function(n) { return Object.assign({}, n); });
    var links = graphData.links.map(function(l) { return {source: l.source, target: l.target, type: l.type}; });

    var simulation = d3.forceSimulation(nodes)
        .force('link', d3.forceLink(links).id(function(d) { return d.id; }).distance(120))
        .force('charge', d3.forceManyBody().strength(-400))
        .force('center', d3.forceCenter(width / 2, height / 2))
        .force('collision', d3.forceCollide().radius(30));

    var link = g.append('g')
        .selectAll('line')
        .data(links)
        .join('line')
        .attr('stroke', '#999')
        .attr('stroke-opacity', 0.5)
        .attr('stroke-width', 1.5)
        .attr('stroke-dasharray', function(d) { return d.type === 'includes' || d.type === 'uses' ? '5,5' : null; })
        .attr('marker-end', 'url(#arrowhead)');

    var node = g.append('g')
        .selectAll('g')
        .data(nodes)
        .join('g')
        .attr('class', 'lineage-node')
        .call(d3.drag()
            .on('start', function(event, d) {
                if (!event.active) simulation.alphaTarget(0.3).restart();
                d.fx = d.x; d.fy = d.y;
            })
            .on('drag', function(event, d) { d.fx = event.x; d.fy = event.y; })
            .on('end', function(event, d) {
                if (!event.active) simulation.alphaTarget(0);
                d.fx = null; d.fy = null;
            })
        );

    node.append('circle')
        .attr('r', function(d) { return radiusMap[d.type] || 14; })
        .attr('fill', function(d) { return colorMap[d.type] || '#666'; })
        .attr('stroke', '#fff')
        .attr('stroke-width', 2)
        .style('cursor', 'pointer');

    node.append('text')
        .text(function(d) { return d.name.length > 18 ? d.name.substring(0, 16) + '...' : d.name; })
        .attr('x', 0)
        .attr('y', function(d) { return (radiusMap[d.type] || 14) + 14; })
        .attr('text-anchor', 'middle')
        .attr('font-size', '11px')
        .attr('fill', 'var(--sst-text)')
        .style('pointer-events', 'none');

    node.on('click', function(event, d) { showDetail(d, graphData); });

    simulation.on('tick', function() {
        link.attr('x1', function(d) { return d.source.x; })
            .attr('y1', function(d) { return d.source.y; })
            .attr('x2', function(d) { return d.target.x; })
            .attr('y2', function(d) { return d.target.y; });
        node.attr('transform', function(d) { return 'translate(' + d.x + ',' + d.y + ')'; });
    });

    initFilters(node, link, links);
    initControls(svg, zoom, width, height);

    window._lineageState = {simulation: simulation, node: node, link: link, links: links, svg: svg, zoom: zoom, width: width, height: height};
}

function initFilters(node, link, links) {
    document.querySelectorAll('.sst-lineage-filters input[type="checkbox"]').forEach(function(cb) {
        cb.addEventListener('change', function() {
            var hidden = {};
            document.querySelectorAll('.sst-lineage-filters input[type="checkbox"]').forEach(function(c) {
                if (!c.checked) hidden[c.value] = true;
            });
            node.style('display', function(d) { return hidden[d.type] ? 'none' : null; });
            link.style('display', function(d) {
                var src = typeof d.source === 'object' ? d.source : {type:''};
                var tgt = typeof d.target === 'object' ? d.target : {type:''};
                return (hidden[src.type] || hidden[tgt.type]) ? 'none' : null;
            });
        });
    });
}

function initControls(svg, zoom, width, height) {
    var zoomIn = document.getElementById('zoom-in');
    var zoomOut = document.getElementById('zoom-out');
    var zoomFit = document.getElementById('zoom-fit');
    if (zoomIn) zoomIn.addEventListener('click', function() { svg.transition().call(zoom.scaleBy, 1.3); });
    if (zoomOut) zoomOut.addEventListener('click', function() { svg.transition().call(zoom.scaleBy, 0.7); });
    if (zoomFit) zoomFit.addEventListener('click', function() {
        svg.transition().call(zoom.transform, d3.zoomIdentity.translate(width/2, height/2).scale(0.8).translate(-width/2, -height/2));
    });
}

function showDetail(d, graphData) {
    var panel = document.getElementById('detail-panel');
    if (!panel) return;

    var upstream = findConnected(d.id, graphData, 'upstream');
    var downstream = findConnected(d.id, graphData, 'downstream');

    var html = '<h2>' + d.name + '</h2>';
    html += '<span class="sst-badge sst-badge-' + d.type + '">' + d.type.replace('_', ' ') + '</span>';

    if (d.metadata) {
        if (d.metadata.description) html += '<p style="margin-top:0.75rem">' + d.metadata.description + '</p>';
        if (d.metadata.expression) html += '<div class="sst-detail-section"><h4>Expression</h4><pre>' + d.metadata.expression + '</pre></div>';
        if (d.metadata.question) html += '<div class="sst-detail-section"><h4>Question</h4><p>' + d.metadata.question + '</p></div>';
        if (d.metadata.sql) html += '<div class="sst-detail-section"><h4>SQL</h4><pre>' + d.metadata.sql + '</pre></div>';
        if (d.metadata.left_table) html += '<div class="sst-detail-section"><h4>Join</h4><p>' + d.metadata.left_table + ' &rarr; ' + d.metadata.right_table + '</p></div>';
        if (d.metadata.synonyms && d.metadata.synonyms.length) html += '<div class="sst-detail-section"><h4>Synonyms</h4><p>' + d.metadata.synonyms.join(', ') + '</p></div>';
        if (d.metadata.source_file) html += '<div class="sst-detail-section"><h4>Source</h4><p style="font-size:0.8rem;color:var(--sst-text-muted)">' + d.metadata.source_file + '</p></div>';
    }

    if (upstream.length) {
        html += '<div class="sst-detail-section"><h4>Upstream (' + upstream.length + ')</h4><ul class="dep-list">';
        upstream.forEach(function(n) { html += '<li data-node-id="' + n.id + '"><span class="sst-badge sst-badge-' + n.type + '">' + n.type.replace('_',' ') + '</span> ' + n.name + '</li>'; });
        html += '</ul></div>';
    }
    if (downstream.length) {
        html += '<div class="sst-detail-section"><h4>Downstream (' + downstream.length + ')</h4><ul class="dep-list">';
        downstream.forEach(function(n) { html += '<li data-node-id="' + n.id + '"><span class="sst-badge sst-badge-' + n.type + '">' + n.type.replace('_',' ') + '</span> ' + n.name + '</li>'; });
        html += '</ul></div>';
    }

    panel.querySelector('.sst-detail-content').innerHTML = html;
    panel.classList.add('open');

    panel.querySelectorAll('.dep-list li').forEach(function(li) {
        li.addEventListener('click', function() {
            var nodeId = this.dataset.nodeId;
            var s = window._lineageState;
            if (s && s.simulation) {
                var target = s.simulation.nodes().find(function(n) { return n.id === nodeId; });
                if (target) {
                    s.svg.transition().duration(500).call(s.zoom.transform,
                        d3.zoomIdentity.translate(s.width/2 - target.x, s.height/2 - target.y));
                }
            }
        });
    });
}

function findConnected(nodeId, graphData, direction) {
    var result = [];
    var nodeMap = {};
    graphData.nodes.forEach(function(n) { nodeMap[n.id] = n; });
    graphData.links.forEach(function(l) {
        var src = typeof l.source === 'object' ? l.source.id : l.source;
        var tgt = typeof l.target === 'object' ? l.target.id : l.target;
        if (direction === 'upstream' && src === nodeId && nodeMap[tgt]) result.push(nodeMap[tgt]);
        if (direction === 'downstream' && tgt === nodeId && nodeMap[src]) result.push(nodeMap[src]);
    });
    return result;
}

function closeDetailPanel() {
    var panel = document.getElementById('detail-panel');
    if (panel) panel.classList.remove('open');
}
