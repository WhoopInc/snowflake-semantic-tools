var currentView = 'catalog';
var currentFilter = 'all';
var currentScopeView = null;

function switchView(view) {
    currentView = view;
    document.querySelectorAll('.view').forEach(function(v) { v.classList.remove('active'); });
    document.getElementById('view-' + view).classList.add('active');
    document.querySelectorAll('.sidebar-item[data-view]').forEach(function(i) { i.classList.remove('active'); });
    var item = document.querySelector('.sidebar-item[data-view="' + view + '"]');
    if (item) item.classList.add('active');
    var title = view === 'catalog' ? 'Catalog' : 'Lineage';
    if (view === 'lineage' && currentScopeView) title = 'Lineage — ' + currentScopeView;
    document.getElementById('topbar-title').textContent = title;
    if (view === 'lineage' && !window._lineageRendered) {
        renderLineage();
        window._lineageRendered = true;
    }
    if (view === 'lineage' && window._lineageRendered && currentScopeView) {
        _ensureLineageScopeForView(currentScopeView);
    }
    if (view === 'lineage' && window._lineageRendered && !currentScopeView) {
        if (typeof clearScope === 'function') clearScope();
    }
}

function setFilter(type) {
    currentFilter = type;
    if (type === 'all') {
        currentScopeView = null;
        if (window._lineageRendered && typeof clearScope === 'function') clearScope();
    }
    document.querySelectorAll('.filter-pill').forEach(function(p) { p.classList.remove('active'); });
    var pill = document.querySelector('.filter-pill[data-filter="' + type + '"]');
    if (pill) pill.classList.add('active');
    renderEntityTable();
}

function filterByType(type) {
    switchView('catalog');
    setFilter(type);
}

function filterByView(viewName) {
    currentScopeView = viewName;
    switchView('catalog');
    setFilter('semantic_view:' + viewName);
    _ensureLineageScopeForView(viewName);
}

function getEntityRows() {
    var rows = [];
    var cat = CATALOG_DATA;
    (cat.semantic_views || []).forEach(function(sv) {
        var tables = Array.isArray(sv.tables) ? sv.tables : [];
        rows.push({type:'semantic_view', name:sv.name||'', desc:sv.description||'', refs:tables.join(', '), data:sv, svName:sv.name});
    });
    (cat.tables || []).forEach(function(t) {
        var pk = t.primary_key || '';
        var loc = [t.database, t.schema].filter(Boolean).join('.');
        rows.push({type:'table', name:t.table_name||'', desc:t.description||'', refs:loc + (pk ? ' PK:'+pk : ''), data:t});
    });
    (cat.metrics || []).forEach(function(m) {
        var tables = Array.isArray(m.tables) ? m.tables : [m.table_name||''];
        rows.push({type:'metric', name:m.name||'', desc:m.description||'', refs:tables.filter(Boolean).join(', '), data:m});
    });
    (cat.relationships || []).forEach(function(r) {
        rows.push({type:'relationship', name:r.relationship_name||'', desc:(r.left_table_name||'')+' \u2192 '+(r.right_table_name||''), refs:'', data:r});
    });
    (cat.filters || []).forEach(function(f) {
        rows.push({type:'filter', name:f.name||'', desc:f.description||'', refs:f.table_name||'', data:f});
    });
    (cat.custom_instructions || []).forEach(function(ci) {
        rows.push({type:'custom_instruction', name:ci.name||'', desc:ci.sql_generation||ci.question_categorization||'', refs:'', data:ci});
    });
    (cat.verified_queries || []).forEach(function(vq) {
        var tables = Array.isArray(vq.tables) ? vq.tables : [];
        rows.push({type:'verified_query', name:vq.name||'', desc:vq.question||'', refs:tables.join(', '), data:vq});
    });
    return rows;
}

function renderEntityTable() {
    var tbody = document.getElementById('entity-tbody');
    var rows = getEntityRows();
    var svFilter = null;
    if (currentFilter.startsWith('semantic_view:')) {
        svFilter = currentFilter.split(':').slice(1).join(':');
        var svDef = (CATALOG_DATA.semantic_views || []).find(function(s) { return s.name === svFilter; });
        var svTables = svDef ? (Array.isArray(svDef.tables) ? svDef.tables.map(function(t){return t.toUpperCase()}) : []) : [];
        rows = rows.filter(function(r) {
            if (r.type === 'semantic_view') return r.name === svFilter;
            if (r.type === 'table') return svTables.indexOf(r.name.toUpperCase()) !== -1;
            var rTables = (r.data.tables || [r.data.table_name || r.data.left_table_name || '']).map(function(t){return (t||'').toUpperCase()});
            return rTables.some(function(t){ return svTables.indexOf(t) !== -1; });
        });
    } else if (currentFilter !== 'all') {
        rows = rows.filter(function(r) { return r.type === currentFilter; });
    }
    tbody.innerHTML = rows.map(function(r) {
        return '<tr onclick="showDetail(\'' + r.type + '\',\'' + escHtml(r.name) + '\')">' +
            '<td><span class="type-badge type-badge-' + r.type + '">' + r.type.replace('_',' ') + '</span></td>' +
            '<td><span class="cell-name">' + escHtml(r.name) + '</span></td>' +
            '<td><span class="cell-desc">' + escHtml(r.desc) + '</span></td>' +
            '<td>' + escHtml(r.refs) + '</td></tr>';
    }).join('');
    if (rows.length === 0) tbody.innerHTML = '<tr><td colspan="4" class="empty-state">No components found.</td></tr>';
}

function showDetail(type, name) {
    var cat = CATALOG_DATA;
    var item = null;
    var lists = {table:'tables',metric:'metrics',relationship:'relationships',filter:'filters',
        semantic_view:'semantic_views',custom_instruction:'custom_instructions',verified_query:'verified_queries'};
    var listKey = lists[type];
    if (listKey) {
        (cat[listKey]||[]).forEach(function(i) {
            var n = i.name || i.table_name || i.relationship_name || '';
            if (n === name) item = i;
        });
    }
    if (!item) return;
    document.getElementById('detail-name').textContent = name;
    document.getElementById('detail-type-badge').innerHTML = '<span class="type-badge type-badge-' + type + '">' + type.replace('_',' ') + '</span>';
    var body = '';
    if (item.description) body += '<div class="detail-section"><h4>Description</h4><p>' + escHtml(item.description) + '</p></div>';
    if (item.expr) body += '<div class="detail-section"><h4>Expression</h4><pre>' + escHtml(item.expr) + '</pre></div>';
    if (item.question) body += '<div class="detail-section"><h4>Question</h4><p>' + escHtml(item.question) + '</p></div>';
    if (item.sql) body += '<div class="detail-section"><h4>SQL</h4><pre>' + escHtml(item.sql) + '</pre></div>';
    if (item.left_table_name) body += '<div class="detail-section"><h4>Join</h4><p>' + escHtml(item.left_table_name) + ' &rarr; ' + escHtml(item.right_table_name||'') + '</p></div>';
    if (item.question_categorization) body += '<div class="detail-section"><h4>Question Categorization</h4><p>' + escHtml(item.question_categorization) + '</p></div>';
    if (item.sql_generation) body += '<div class="detail-section"><h4>SQL Generation</h4><p>' + escHtml(item.sql_generation) + '</p></div>';
    if (item.primary_key) body += '<div class="detail-section"><h4>Primary Key</h4><p>' + escHtml(String(item.primary_key)) + '</p></div>';
    if (item.database && item.schema) body += '<div class="detail-section"><h4>Location</h4><p>' + escHtml(item.database) + '.' + escHtml(item.schema) + '</p></div>';
    if (item.synonyms && item.synonyms.length) body += '<div class="detail-section"><h4>Synonyms</h4><p>' + item.synonyms.map(escHtml).join(', ') + '</p></div>';
    var nodeId = type + ':' + name.toUpperCase();
    var upstream = findConnected(nodeId, 'upstream');
    var downstream = findConnected(nodeId, 'downstream');
    if (upstream.length) {
        body += '<div class="detail-section"><h4>Upstream (' + upstream.length + ')</h4><ul class="detail-deps">';
        upstream.forEach(function(n) { body += '<li onclick="showDetail(\'' + n.type + '\',\'' + escHtml(n.name) + '\')"><span class="type-badge type-badge-' + n.type + '" style="font-size:9px">' + n.type.replace('_',' ') + '</span>' + escHtml(n.name) + '</li>'; });
        body += '</ul></div>';
    }
    if (downstream.length) {
        body += '<div class="detail-section"><h4>Downstream (' + downstream.length + ')</h4><ul class="detail-deps">';
        downstream.forEach(function(n) { body += '<li onclick="showDetail(\'' + n.type + '\',\'' + escHtml(n.name) + '\')"><span class="type-badge type-badge-' + n.type + '" style="font-size:9px">' + n.type.replace('_',' ') + '</span>' + escHtml(n.name) + '</li>'; });
        body += '</ul></div>';
    }
    if (item.source_file) body += '<div class="detail-section"><h4>Source File</h4><p style="font-size:12px;color:var(--text-muted)">' + escHtml(item.source_file) + '</p></div>';
    body += '<div class="detail-section"><a href="#" onclick="event.preventDefault();switchView(\'lineage\');focusLineageNode(\'' + nodeId + '\')" style="font-size:12px;color:var(--accent)">View in Lineage &rarr;</a></div>';
    document.getElementById('detail-body').innerHTML = body;
    document.getElementById('detail-panel').classList.add('open');
}

function closeDetail() { document.getElementById('detail-panel').classList.remove('open'); }

function findConnected(nodeId, direction) {
    var result = [];
    var nodeMap = {};
    GRAPH_DATA.nodes.forEach(function(n) { nodeMap[n.id] = n; });
    GRAPH_DATA.links.forEach(function(l) {
        var s = typeof l.source === 'object' ? l.source.id : l.source;
        var t = typeof l.target === 'object' ? l.target.id : l.target;
        if (direction === 'upstream' && s === nodeId && nodeMap[t]) result.push(nodeMap[t]);
        if (direction === 'downstream' && t === nodeId && nodeMap[s]) result.push(nodeMap[s]);
    });
    return result;
}

function openSearch() {
    document.getElementById('search-overlay').classList.add('open');
    var input = document.getElementById('search-input');
    input.value = '';
    input.focus();
    renderSearchResults('');
}
function closeSearch() { document.getElementById('search-overlay').classList.remove('open'); }

document.addEventListener('keydown', function(e) {
    if ((e.metaKey || e.ctrlKey) && e.key === 'k') { e.preventDefault(); openSearch(); }
    if (e.key === 'Escape') { closeSearch(); closeDetail(); }
});
document.getElementById('search-overlay').addEventListener('click', function(e) { if (e.target === this) closeSearch(); });
document.getElementById('search-input').addEventListener('input', function() { renderSearchResults(this.value.trim().toLowerCase()); });

function renderSearchResults(q) {
    var el = document.getElementById('search-results');
    if (!q) { el.innerHTML = '<div class="search-empty">Type to search...</div>'; return; }
    var matches = SEARCH_DATA.filter(function(i) {
        return i.name.toLowerCase().indexOf(q)!==-1 || (i.description||'').toLowerCase().indexOf(q)!==-1 ||
            (i.expression||'').toLowerCase().indexOf(q)!==-1 || (i.synonyms||[]).some(function(s){return s.toLowerCase().indexOf(q)!==-1;});
    }).slice(0, 15);
    if (!matches.length) { el.innerHTML = '<div class="search-empty">No results</div>'; return; }
    el.innerHTML = matches.map(function(i) {
        return '<div class="search-result" onclick="closeSearch();showDetail(\'' + i.type + '\',\'' + escHtml(i.name) + '\')">' +
            '<span class="type-badge type-badge-' + i.type + '" style="font-size:9px">' + i.type.replace('_',' ') + '</span>' +
            '<div><div class="search-result-name">' + escHtml(i.name) + '</div>' +
            (i.description ? '<div class="search-result-desc">' + escHtml(i.description) + '</div>' : '') + '</div></div>';
    }).join('');
}

function _walkGraphAll(startId) {
    var visited = {};
    var adj = {};
    GRAPH_DATA.links.forEach(function(l) {
        var s = typeof l.source === 'object' ? l.source.id : l.source;
        var t = typeof l.target === 'object' ? l.target.id : l.target;
        if (!adj[s]) adj[s] = [];
        if (!adj[t]) adj[t] = [];
        adj[s].push(t);
        adj[t].push(s);
    });
    var queue = [startId];
    visited[startId] = true;
    while (queue.length) {
        var cur = queue.shift();
        (adj[cur] || []).forEach(function(nb) {
            if (!visited[nb]) { visited[nb] = true; queue.push(nb); }
        });
    }
    return visited;
}

function _ensureLineageScopeForView(viewName) {
    if (!window._lineageRendered) return;
    var svId = 'semantic_view:' + viewName.toUpperCase();
    var reachable = _walkGraphAll(svId);
    if (typeof scopeToNodes === 'function') scopeToNodes(reachable);
}

function escHtml(s) { var d=document.createElement('div'); d.textContent=s||''; return d.innerHTML; }

document.addEventListener('DOMContentLoaded', function() { renderEntityTable(); });
document.addEventListener('click', function(e) {
    var panel = document.getElementById('detail-panel');
    if (panel.classList.contains('open') && !panel.contains(e.target) && !e.target.closest('.entity-table tbody tr') && !e.target.closest('.lineage-node') && !e.target.closest('.detail-deps li')) {
        closeDetail();
    }
});
