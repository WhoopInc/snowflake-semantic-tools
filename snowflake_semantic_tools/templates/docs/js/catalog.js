document.addEventListener('DOMContentLoaded', function() {
    initTabs();
    initSearch();
    initCards();
});

function initTabs() {
    var tabs = document.querySelectorAll('.sst-tab');
    var panels = document.querySelectorAll('.sst-tab-panel');
    tabs.forEach(function(tab) {
        tab.addEventListener('click', function() {
            var target = this.dataset.tab;
            tabs.forEach(function(t) { t.classList.remove('active'); });
            panels.forEach(function(p) { p.style.display = 'none'; });
            this.classList.add('active');
            var panel = document.getElementById('panel-' + target);
            if (panel) panel.style.display = 'block';
        });
    });
}

function initCards() {
    document.querySelectorAll('.sst-card-header').forEach(function(header) {
        header.addEventListener('click', function() {
            this.closest('.sst-card').classList.toggle('expanded');
        });
    });
}

function initSearch() {
    var overlay = document.getElementById('search-overlay');
    var input = document.getElementById('search-input');
    var results = document.getElementById('search-results');
    if (!overlay || !input) return;

    var searchData = [];
    try { searchData = JSON.parse(document.getElementById('search-data').textContent); } catch(e) {}

    function openSearch() {
        overlay.classList.add('active');
        input.value = '';
        input.focus();
        renderResults('');
    }
    function closeSearch() {
        overlay.classList.remove('active');
    }

    document.addEventListener('keydown', function(e) {
        if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
            e.preventDefault();
            openSearch();
        }
        if (e.key === 'Escape') closeSearch();
    });

    var trigger = document.querySelector('.sst-search-trigger');
    if (trigger) trigger.addEventListener('click', openSearch);

    overlay.addEventListener('click', function(e) {
        if (e.target === overlay) closeSearch();
    });

    var activeIdx = -1;
    input.addEventListener('input', function() {
        activeIdx = -1;
        renderResults(this.value.trim().toLowerCase());
    });

    input.addEventListener('keydown', function(e) {
        var items = results.querySelectorAll('.sst-search-item');
        if (e.key === 'ArrowDown') { e.preventDefault(); activeIdx = Math.min(activeIdx + 1, items.length - 1); highlightItem(items); }
        if (e.key === 'ArrowUp') { e.preventDefault(); activeIdx = Math.max(activeIdx - 1, 0); highlightItem(items); }
        if (e.key === 'Enter' && activeIdx >= 0 && items[activeIdx]) {
            e.preventDefault();
            var card = document.getElementById(items[activeIdx].dataset.cardId);
            if (card) { closeSearch(); card.scrollIntoView({behavior:'smooth',block:'center'}); card.classList.add('expanded'); }
        }
    });

    function highlightItem(items) {
        items.forEach(function(item, i) {
            item.classList.toggle('active', i === activeIdx);
        });
    }

    function renderResults(query) {
        if (!query) {
            results.innerHTML = '<div class="sst-search-empty">Start typing to search components...</div>';
            return;
        }
        var matches = searchData.filter(function(item) {
            return item.name.toLowerCase().indexOf(query) !== -1 ||
                   (item.description || '').toLowerCase().indexOf(query) !== -1 ||
                   (item.expression || '').toLowerCase().indexOf(query) !== -1 ||
                   (item.synonyms || []).some(function(s) { return s.toLowerCase().indexOf(query) !== -1; });
        }).slice(0, 20);

        if (matches.length === 0) {
            results.innerHTML = '<div class="sst-search-empty">No results for "' + query + '"</div>';
            return;
        }
        results.innerHTML = matches.map(function(item, i) {
            return '<div class="sst-search-item" data-card-id="card-' + item.type + '-' + item.id + '">' +
                '<span class="sst-badge sst-badge-' + item.type + '">' + item.type.replace('_', ' ') + '</span>' +
                '<div><div class="sst-search-item-name">' + item.name + '</div>' +
                (item.description ? '<div class="sst-search-item-desc">' + item.description + '</div>' : '') +
                '</div></div>';
        }).join('');
    }
}
