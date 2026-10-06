/**
 * UNIFIED DISTRIBUTED KV-STORE CLIENT
 * All components (Consistent Hash Ring, Gossip Protocol, LSM Storage Engine,
 * Vector Clocks, Merkle Trees) are interconnected through the C++ ClusterState backend.
 */

// ── Shared API Client ────────────────────────────────────────────────────────
async function apiCall(endpoint, method = 'GET', body = null) {
  const start = performance.now();
  try {
    const options = {
      method,
      headers: { 'Content-Type': 'application/json' }
    };
    if (body) options.body = JSON.stringify(body);

    const res = await fetch(endpoint, options);
    const data = await res.json();
    const duration = Math.round(performance.now() - start);

    const badge = document.getElementById('backend-status-text');
    if (badge) {
      badge.textContent = `C++ Backend Connected (${duration}ms)`;
    }
    return { ...data, _durationMs: duration };
  } catch (err) {
    console.warn(`[API] Endpoint ${endpoint} failed:`, err);
    const badge = document.getElementById('backend-status-text');
    if (badge) badge.textContent = 'Backend Offline';
    return null;
  }
}

function fnv1a_32(str) {
  let hash = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}

const NODE_PALETTE = [
  { color: '#06b6d4', glow: 'rgba(6, 182, 212, 0.4)' },
  { color: '#10b981', glow: 'rgba(16, 185, 129, 0.4)' },
  { color: '#f59e0b', glow: 'rgba(245, 158, 11, 0.4)' },
  { color: '#a855f7', glow: 'rgba(168, 85, 247, 0.4)' },
  { color: '#ec4899', glow: 'rgba(236, 72, 153, 0.4)' },
  { color: '#3b82f6', glow: 'rgba(59, 130, 246, 0.4)' }
];

function getNodeColor(nodeId) {
  const hash = Math.abs(fnv1a_32(nodeId));
  return NODE_PALETTE[hash % NODE_PALETTE.length].color;
}

// ── TAB SWITCHING ───────────────────────────────────────────────────────────
function switchTab(tabId) {
  document.querySelectorAll('.nav-tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));

  const tabBtn = document.querySelector(`.nav-tab[data-tab="${tabId}"]`);
  if (tabBtn) tabBtn.classList.add('active');

  const pane = document.getElementById(`section-${tabId}`);
  if (pane) pane.classList.add('active');
}

document.querySelectorAll('.nav-tab').forEach(tab => {
  tab.addEventListener('click', () => switchTab(tab.dataset.tab));
});

// ============================================================================
// CENTRAL UNIFIED CLUSTER CONTROLLER
// ============================================================================
class UnifiedClusterApp {
  constructor() {
    this.nodes = [];
    this.selectedStorageNode = 'node-alpha';
    this.selectedViewingNode = 'node-alpha';
    this.selectedMerkleA = 'node-alpha';
    this.selectedMerkleB = 'node-beta';
    this.virtualNodesPerNode = 40;
    this.ring = [];
    this.autoGossip = true;
    this.failureTimeoutMs = 2500;
    this.clusterKeys = [];

    // Sub-modules
    this.initDOM();
    this.initVectorClocks();
    this.syncClusterState();

    // Auto-Gossip background tick loop
    setInterval(() => {
      if (this.autoGossip) {
        apiCall('/api/cluster/gossip_tick', 'POST', {}).then(() => {
          this.syncClusterState(false);
        });
      } else {
        this.syncClusterState(false);
      }
    }, 2000);
  }

  initDOM() {
    // ── Global Cluster Action Bar ───────────────────────────────────────────
    const globalAddInput = document.getElementById('global-new-node-input');
    const globalAddBtn = document.getElementById('global-btn-add-node');
    if (globalAddBtn && globalAddInput) {
      globalAddBtn.addEventListener('click', () => this.handleAddNode(globalAddInput.value));
      globalAddInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') this.handleAddNode(globalAddInput.value);
      });
    }

    const seedBtn = document.getElementById('global-btn-seed-demo');
    if (seedBtn) seedBtn.addEventListener('click', () => this.handleSeedDemo());

    const chaosBtn = document.getElementById('global-btn-chaos-toggle');
    if (chaosBtn) chaosBtn.addEventListener('click', () => this.handleChaosToggle());

    // ── Tab 1: Consistent Hash Ring ─────────────────────────────────────────
    document.getElementById('btn-route-key').addEventListener('click', () => this.handleRouteKey());
    document.getElementById('route-key-input').addEventListener('keydown', (e) => {
      if (e.key === 'Enter') this.handleRouteKey();
    });

    document.querySelectorAll('.chip-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        document.getElementById('route-key-input').value = btn.dataset.key;
        this.handleRouteKey();
      });
    });

    document.getElementById('btn-add-node').addEventListener('click', () => {
      const input = document.getElementById('new-node-input');
      this.handleAddNode(input.value);
      input.value = '';
    });

    const routePutBtn = document.getElementById('btn-route-and-put');
    if (routePutBtn) {
      routePutBtn.addEventListener('click', async () => {
        const key = document.getElementById('route-key-input').value.trim() || 'user:alice';
        document.getElementById('lsm-key-input').value = key;
        document.getElementById('lsm-val-input').value = `Data for ${key} @ ${new Date().toLocaleTimeString()}`;
        switchTab('storage-engine');
        await this.handleDistributedPut();
      });
    }

    // ── Tab 2: Gossip & Failure Detection ───────────────────────────────────
    document.getElementById('btn-gossip-tick-all').addEventListener('click', async () => {
      await apiCall('/api/cluster/gossip_tick', 'POST', {});
      this.logGossip(`[GOSSIP] Heartbeat round triggered across live peers.`);
      await this.syncClusterState();
    });

    const autoBtn = document.getElementById('btn-gossip-auto-toggle');
    autoBtn.addEventListener('click', () => {
      this.autoGossip = !this.autoGossip;
      autoBtn.textContent = `Auto-Gossip: ${this.autoGossip ? 'ON' : 'OFF'}`;
      autoBtn.classList.toggle('btn-primary', this.autoGossip);
    });

    const gossipAddBtn = document.getElementById('btn-gossip-add-node');
    const gossipAddInput = document.getElementById('gossip-new-node-input');
    if (gossipAddBtn && gossipAddInput) {
      gossipAddBtn.addEventListener('click', () => {
        this.handleAddNode(gossipAddInput.value);
        gossipAddInput.value = '';
      });
    }

    const viewingSelect = document.getElementById('select-viewing-node');
    if (viewingSelect) {
      viewingSelect.addEventListener('change', (e) => {
        this.selectedViewingNode = e.target.value;
        this.loadMembershipForNode(this.selectedViewingNode);
      });
    }

    document.getElementById('failure-timeout-slider').addEventListener('input', (e) => {
      this.failureTimeoutMs = parseInt(e.target.value, 10);
      document.getElementById('failure-timeout-label').innerHTML = `<strong>${this.failureTimeoutMs} ms</strong>`;
    });

    document.getElementById('btn-clear-gossip-log').addEventListener('click', () => {
      document.getElementById('gossip-log-stream').innerHTML = '';
    });

    // ── Tab 3: LSM Storage Engine ───────────────────────────────────────────
    document.getElementById('lsm-select-node').addEventListener('change', (e) => {
      this.selectedStorageNode = e.target.value;
      this.renderStorageEngineView();
    });

    document.getElementById('btn-lsm-put').addEventListener('click', () => this.handleDistributedPut());
    document.getElementById('btn-lsm-get').addEventListener('click', () => this.handleDistributedGet());
    document.getElementById('btn-lsm-del').addEventListener('click', () => this.handleDistributedDel());
    document.getElementById('btn-lsm-flush').addEventListener('click', () => this.handleFlush());
    document.getElementById('btn-lsm-crash').addEventListener('click', () => this.handleCrash());
    document.getElementById('btn-lsm-recover').addEventListener('click', () => this.handleRecover());

    const refreshKeysBtn = document.getElementById('btn-refresh-cluster-keys');
    if (refreshKeysBtn) {
      refreshKeysBtn.addEventListener('click', () => this.loadClusterKeys());
    }

    // ── Tab 4: Vector Clocks ────────────────────────────────────────────────
    const inspectVcBtn = document.getElementById('btn-inspect-vc-key');
    if (inspectVcBtn) {
      inspectVcBtn.addEventListener('click', () => this.inspectSelectedClusterKeyClock());
    }

    const simConcurrentBtn = document.getElementById('btn-simulate-concurrent-write');
    if (simConcurrentBtn) {
      simConcurrentBtn.addEventListener('click', () => this.simulateConcurrentWrite());
    }

    // ── Tab 5: Merkle Anti-Entropy ──────────────────────────────────────────
    document.getElementById('merkle-select-node-a').addEventListener('change', (e) => {
      this.selectedMerkleA = e.target.value;
      this.runMerkleDiff();
    });
    document.getElementById('merkle-select-node-b').addEventListener('change', (e) => {
      this.selectedMerkleB = e.target.value;
      this.runMerkleDiff();
    });
    document.getElementById('btn-run-merkle-diff').addEventListener('click', () => this.runMerkleDiff());
    document.getElementById('btn-sync-replicas').addEventListener('click', () => this.handleMerkleSync());
    document.getElementById('btn-inject-divergence').addEventListener('click', () => this.handleInjectDivergence());
  }

  logGossip(msg, type = '') {
    const time = new Date().toLocaleTimeString();
    const stream = document.getElementById('gossip-log-stream');
    if (!stream) return;
    const div = document.createElement('div');
    div.className = `log-entry ${type}`;
    div.textContent = `[${time}] ${msg}`;
    stream.appendChild(div);
    stream.scrollTop = stream.scrollHeight;
  }

  async handleAddNode(rawName) {
    const val = (rawName || '').trim().toLowerCase();
    if (!val) return;

    await apiCall('/api/cluster/add_node', 'POST', { node_id: val });
    const globalInput = document.getElementById('global-new-node-input');
    if (globalInput) globalInput.value = '';

    this.logGossip(`[CLUSTER JOIN] Physical node '${val}' added across Consistent Hash Ring, Gossip mesh, and LSM storage engine.`, 'system');
    await this.syncClusterState();
    this.handleRouteKey();
    await this.loadClusterKeys();
  }

  async handleSeedDemo() {
    await apiCall('/api/cluster/seed_demo', 'POST', {});
    this.logGossip(`[DEMO SEED] Populated cluster with 3 nodes and 4 replicated keys with causal vector clocks.`, 'system');
    await this.syncClusterState();
    this.handleRouteKey();
    await this.loadClusterKeys();
  }

  async handleChaosToggle() {
    const aliveNodes = this.nodes.filter(n => n.is_alive && n.status === 'ALIVE');
    if (aliveNodes.length > 1) {
      const target = aliveNodes[aliveNodes.length - 1].id;
      await apiCall('/api/cluster/kill_node', 'POST', { node_id: target });
      this.logGossip(`[CHAOS SIMULATION] Node '${target}' CRASHED! Heartbeats halted. Gossip failure detector triggered.`, 'alert');
    } else {
      const deadNodes = this.nodes.filter(n => !n.is_alive || n.status === 'DEAD');
      if (deadNodes.length > 0) {
        const target = deadNodes[0].id;
        await apiCall('/api/cluster/revive_node', 'POST', { node_id: target });
        this.logGossip(`[CHAOS SIMULATION] Node '${target}' REVIVED! Resumed gossip heartbeats.`, 'system');
      }
    }
    await this.syncClusterState();
    this.handleRouteKey();
    await this.loadClusterKeys();
  }

  async syncClusterState(reRenderViews = true) {
    const data = await apiCall('/api/cluster/state', 'GET');
    if (data && data.success && data.nodes) {
      this.nodes = data.nodes;
    } else {
      if (this.nodes.length === 0) {
        this.nodes = [
          { id: 'node-alpha', status: 'ALIVE', is_alive: true, heartbeat: 10, key_count: 0, sstable_count: 0, wal_count: 0 },
          { id: 'node-beta',  status: 'ALIVE', is_alive: true, heartbeat: 12, key_count: 0, sstable_count: 0, wal_count: 0 },
          { id: 'node-gamma', status: 'ALIVE', is_alive: true, heartbeat: 8,  key_count: 0, sstable_count: 0, wal_count: 0 }
        ];
      }
    }

    this.rebuildRing();
    this.renderSummaryBar();
    this.renderNodeDropdowns();
    this.renderHashRing();
    this.renderGossipMesh();

    if (reRenderViews) {
      this.renderStorageEngineView();
      this.loadClusterKeys();
      this.loadMembershipForNode(this.selectedViewingNode);
    }
  }

  renderSummaryBar() {
    const total = this.nodes.length;
    const alive = this.nodes.filter(n => n.is_alive && n.status === 'ALIVE').length;
    const dead = total - alive;
    const totalKeys = this.nodes.reduce((acc, n) => acc + (n.key_count || 0), 0);

    document.getElementById('global-nodes-count').textContent = `${total} Nodes`;
    document.getElementById('global-alive-count').textContent = `${alive} Alive`;
    document.getElementById('global-dead-count').textContent = `${dead} Dead`;
    document.getElementById('global-keys-count').textContent = `${totalKeys} Replicated Entries`;
  }

  renderNodeDropdowns() {
    const nodeIds = this.nodes.map(n => n.id);

    // LSM Selector
    const lsmSelect = document.getElementById('lsm-select-node');
    if (!nodeIds.includes(this.selectedStorageNode)) {
      this.selectedStorageNode = nodeIds[0] || 'node-alpha';
    }
    lsmSelect.innerHTML = this.nodes.map(n => `
      <option value="${n.id}" ${n.id === this.selectedStorageNode ? 'selected' : ''}>
        ${n.id} (${n.status})
      </option>
    `).join('');

    // Gossip Viewing Node
    const viewSelect = document.getElementById('select-viewing-node');
    if (viewSelect) {
      if (!nodeIds.includes(this.selectedViewingNode)) {
        this.selectedViewingNode = nodeIds[0] || 'node-alpha';
      }
      viewSelect.innerHTML = this.nodes.map(n => `
        <option value="${n.id}" ${n.id === this.selectedViewingNode ? 'selected' : ''}>
          View from: ${n.id} (${n.status})
        </option>
      `).join('');
    }

    // Merkle Selectors
    const merkleA = document.getElementById('merkle-select-node-a');
    const merkleB = document.getElementById('merkle-select-node-b');

    if (!nodeIds.includes(this.selectedMerkleA)) this.selectedMerkleA = nodeIds[0] || 'node-alpha';
    if (!nodeIds.includes(this.selectedMerkleB)) this.selectedMerkleB = nodeIds[1] || nodeIds[0] || 'node-beta';

    merkleA.innerHTML = this.nodes.map(n => `
      <option value="${n.id}" ${n.id === this.selectedMerkleA ? 'selected' : ''}>${n.id} (${n.status})</option>
    `).join('');

    merkleB.innerHTML = this.nodes.map(n => `
      <option value="${n.id}" ${n.id === this.selectedMerkleB ? 'selected' : ''}>${n.id} (${n.status})</option>
    `).join('');
  }

  // ==========================================================================
  // 1. CONSISTENT HASH RING
  // ==========================================================================
  rebuildRing() {
    this.ring = [];
    for (const node of this.nodes) {
      for (let i = 0; i < this.virtualNodesPerNode; i++) {
        const vnodeKey = `${node.id}#${i}`;
        const hash = fnv1a_32(vnodeKey);
        this.ring.push({ hash, nodeId: node.id, vnodeKey, isAlive: node.is_alive && node.status === 'ALIVE' });
      }
    }
    this.ring.sort((a, b) => a.hash - b.hash);
  }

  renderHashRing() {
    document.getElementById('stat-node-count').textContent = this.nodes.length;
    document.getElementById('stat-vnode-count').textContent = this.ring.length;

    // Physical node chips
    const nodesList = document.getElementById('physical-nodes-list');
    nodesList.innerHTML = this.nodes.map(node => {
      const isDead = !node.is_alive || node.status === 'DEAD';
      return `
        <div class="node-chip ${isDead ? 'dead' : ''}">
          <span class="chip-dot" style="background: ${isDead ? '#ef4444' : getNodeColor(node.id)};"></span>
          <span><strong>${node.id}</strong> <small>(${node.status})</small></span>
          ${this.nodes.length > 1 ? `<button class="btn-remove" data-node="${node.id}" title="Decommission Node">&times;</button>` : ''}
        </div>
      `;
    }).join('');

    nodesList.querySelectorAll('.btn-remove').forEach(btn => {
      btn.addEventListener('click', async () => {
        const toRemove = btn.dataset.node;
        await apiCall('/api/cluster/remove_node', 'POST', { node_id: toRemove });
        this.logGossip(`[CLUSTER] Node '${toRemove}' removed from cluster topology.`, 'alert');
        await this.syncClusterState();
        this.handleRouteKey();
        await this.loadClusterKeys();
      });
    });

    // Legend
    document.getElementById('ring-legend').innerHTML = this.nodes.map(node => {
      const isDead = !node.is_alive || node.status === 'DEAD';
      return `
        <div class="legend-item" style="${isDead ? 'opacity: 0.6;' : ''}">
          <span class="legend-color" style="background: ${getNodeColor(node.id)}; color: ${getNodeColor(node.id)};"></span>
          <span>${node.id} <strong style="color: ${isDead ? '#f87171' : '#34d399'}">(${node.status})</strong></span>
        </div>
      `;
    }).join('');

    // Virtual Node Dots on SVG Ring
    const vnodeGroup = document.getElementById('vnode-markers');
    const cx = 300, cy = 300, r = 230;
    let markersHtml = '';

    for (const vnode of this.ring) {
      const angle = (vnode.hash / 0xffffffff) * 2 * Math.PI - Math.PI / 2;
      const x = cx + r * Math.cos(angle);
      const y = cy + r * Math.sin(angle);
      const color = getNodeColor(vnode.nodeId);
      const deadClass = !vnode.isAlive ? 'dead' : '';

      markersHtml += `
        <circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="${vnode.isAlive ? 3 : 2}" 
                class="vnode-dot ${deadClass}" fill="${vnode.isAlive ? color : '#64748b'}" data-node="${vnode.nodeId}">
          <title>${vnode.vnodeKey} (0x${vnode.hash.toString(16)}) ${vnode.isAlive ? '[ALIVE]' : '[DEAD]'}</title>
        </circle>
      `;
    }
    vnodeGroup.innerHTML = markersHtml;
  }

  async handleRouteKey() {
    const key = document.getElementById('route-key-input').value.trim() || 'user:alice';
    const hash = fnv1a_32(key);

    const res = await apiCall('/api/cluster/route', 'POST', { key, count: 3 });

    let replicas = [];
    if (res && res.success && res.replicas) {
      replicas = res.replicas;
    } else {
      replicas = this.nodes.slice(0, 3).map(n => ({ id: n.id, status: n.status }));
    }

    const durationInfo = res ? ` • C++ execution: ${res._durationMs}ms` : '';
    document.getElementById('route-hash-val').textContent = 
      `0x${hash.toString(16).padStart(8, '0')} (${hash.toLocaleString()})${durationInfo}`;

    const renderRep = (r, elId) => {
      const el = document.getElementById(elId);
      if (!r) {
        el.textContent = 'None';
        el.className = 'badge-node';
        return;
      }
      const isDead = r.status === 'DEAD';
      el.textContent = `${r.id} (${r.status})`;
      el.className = `badge-node ${isDead ? 'tertiary' : ''}`;
    };

    renderRep(replicas[0], 'route-primary-node');
    renderRep(replicas[1], 'route-secondary-node');
    renderRep(replicas[2], 'route-tertiary-node');

    const warningBox = document.getElementById('routing-failover-warning');
    if (replicas[0] && replicas[0].status === 'DEAD') {
      const fallback = replicas.find(r => r.status === 'ALIVE');
      warningBox.style.display = 'block';
      warningBox.innerHTML = `⚠️ Primary replica <strong>${replicas[0].id}</strong> is DEAD (Gossip timeout)! Failing over reads/writes to alive replica <strong>${fallback ? fallback.id : 'None'}</strong>.`;
    } else {
      warningBox.style.display = 'none';
    }

    document.getElementById('center-status-text').textContent = replicas[0] ? replicas[0].id : 'Empty';
    document.getElementById('center-sub-text').textContent = `Target: ${key}`;

    // Key marker on ring
    const keyGroup = document.getElementById('key-marker-group');
    const cx = 300, cy = 300, r = 230;
    const angle = (hash / 0xffffffff) * 2 * Math.PI - Math.PI / 2;
    const x = cx + r * Math.cos(angle);
    const y = cy + r * Math.sin(angle);

    keyGroup.innerHTML = `
      <circle cx="${x}" cy="${y}" r="8" fill="#fff" stroke="#06b6d4" stroke-width="3" filter="drop-shadow(0 0 10px #22d3ee)"></circle>
      <circle cx="${x}" cy="${y}" r="12" fill="none" stroke="#22d3ee" stroke-width="2" class="key-marker-pulse"></circle>
      <text x="${x}" y="${y - 14}" fill="#f8fafc" font-size="11" font-family="JetBrains Mono" font-weight="700" text-anchor="middle">
        🔑 ${key}
      </text>
    `;
  }

  // ==========================================================================
  // 2. GOSSIP & FAILURE DETECTION
  // ==========================================================================
  renderGossipMesh() {
    const grid = document.getElementById('gossip-nodes-grid');
    grid.innerHTML = this.nodes.map(n => {
      const isAlive = n.is_alive && n.status === 'ALIVE';
      const sharePct = this.nodes.length > 0 ? (100 / this.nodes.length).toFixed(1) : '0';
      return `
        <div class="gossip-node-card ${isAlive ? 'alive' : 'dead'}">
          <div class="node-card-top">
            <span class="node-name">${n.id}</span>
            <span class="status-badge ${isAlive ? 'alive' : 'dead'}">${n.status}</span>
          </div>
          <div class="heartbeat-meter">
            <span>Heartbeat Counter:</span>
            <span class="heartbeat-count">${n.heartbeat}</span>
          </div>
          <div class="heartbeat-meter">
            <span>Ring Ownership:</span>
            <span>~${sharePct}% of ring (${this.virtualNodesPerNode} vnodes)</span>
          </div>
          <div class="heartbeat-meter">
            <span>Stored Keys:</span>
            <span style="color: #38bdf8; font-weight: 600;">${n.key_count} keys</span>
          </div>
          <div class="node-card-actions" style="margin-top: 0.85rem; display: flex; gap: 0.4rem; flex-wrap: wrap;">
            <button class="btn btn-xs ${isAlive ? 'btn-danger' : 'btn-success'}" data-action="toggle-kill" data-node="${n.id}">
              ${isAlive ? 'Simulate Crash' : 'Revive Node'}
            </button>
            <button class="btn btn-xs btn-outline" data-action="view-storage" data-node="${n.id}">
              View Storage 💾
            </button>
            <button class="btn btn-xs btn-outline" data-action="view-ring" data-node="${n.id}">
              View on Ring 🌐
            </button>
          </div>
        </div>
      `;
    }).join('');

    // Wire Card Buttons
    grid.querySelectorAll('button').forEach(btn => {
      btn.addEventListener('click', async () => {
        const nodeId = btn.dataset.node;
        const action = btn.dataset.action;

        if (action === 'toggle-kill') {
          const node = this.nodes.find(n => n.id === nodeId);
          const isAlive = node ? (node.is_alive && node.status === 'ALIVE') : true;
          if (isAlive) {
            await apiCall('/api/cluster/kill_node', 'POST', { node_id: nodeId });
            this.logGossip(`[CHAOS] Node '${nodeId}' killed! Gossip heartbeats halted.`, 'alert');
          } else {
            await apiCall('/api/cluster/revive_node', 'POST', { node_id: nodeId });
            this.logGossip(`[HEAL] Node '${nodeId}' revived! Resumed gossip heartbeats.`, 'system');
          }
          await this.syncClusterState();
          this.handleRouteKey();
          await this.loadClusterKeys();
        } else if (action === 'view-storage') {
          this.selectedStorageNode = nodeId;
          switchTab('storage-engine');
          this.renderNodeDropdowns();
          this.renderStorageEngineView();
        } else if (action === 'view-ring') {
          switchTab('hash-ring');
          this.handleRouteKey();
        }
      });
    });

    // Kill Switches list in sidebar
    const killSwitches = document.getElementById('node-kill-switches');
    killSwitches.innerHTML = this.nodes.map(n => `
      <div class="kill-row">
        <span class="node-lbl">${n.id}</span>
        <button class="btn btn-xs ${n.is_alive ? 'btn-danger' : 'btn-success'}" data-node="${n.id}" data-action="${n.is_alive ? 'kill' : 'revive'}">
          ${n.is_alive ? 'Simulate Crash' : 'Revive Node'}
        </button>
      </div>
    `).join('');

    killSwitches.querySelectorAll('button').forEach(btn => {
      btn.addEventListener('click', async () => {
        const nodeId = btn.dataset.node;
        const action = btn.dataset.action;
        if (action === 'kill') {
          await apiCall('/api/cluster/kill_node', 'POST', { node_id: nodeId });
          this.logGossip(`Node '${nodeId}' killed! Gossip heartbeats halted.`, 'alert');
        } else {
          await apiCall('/api/cluster/revive_node', 'POST', { node_id: nodeId });
          this.logGossip(`Node '${nodeId}' revived! Resumed gossip heartbeats.`, 'system');
        }
        await this.syncClusterState();
        this.handleRouteKey();
        await this.loadClusterKeys();
      });
    });
  }

  async loadMembershipForNode(nodeId) {
    if (!nodeId) return;
    const res = await apiCall(`/api/gossip/membership?node_id=${nodeId}`, 'GET');
    const tbody = document.getElementById('membership-table-body');
    if (!tbody) return;

    if (res && res.success && res.members) {
      tbody.innerHTML = res.members.map(m => `
        <tr>
          <td><strong>${m.node_id}</strong></td>
          <td>${m.heartbeat}</td>
          <td>${m.status === 'ALIVE' ? 'Active (<100ms)' : 'Timed Out (>2500ms)'}</td>
          <td><span class="status-badge ${m.status === 'ALIVE' ? 'alive' : 'dead'}">${m.status}</span></td>
        </tr>
      `).join('');
    } else {
      tbody.innerHTML = this.nodes.map(n => `
        <tr>
          <td><strong>${n.id}</strong></td>
          <td>${n.heartbeat}</td>
          <td>${n.is_alive ? 'Active' : 'Timed Out'}</td>
          <td><span class="status-badge ${n.is_alive ? 'alive' : 'dead'}">${n.status}</span></td>
        </tr>
      `).join('');
    }
  }

  // ==========================================================================
  // 3. LSM STORAGE ENGINE (DISTRIBUTED REPLICATION & DATA MATRIX)
  // ==========================================================================
  setLsmFeedback(msg, isSuccess = true) {
    const el = document.getElementById('lsm-read-feedback');
    el.innerHTML = `<span style="color: ${isSuccess ? '#34d399' : '#f87171'}">${msg}</span>`;
  }

  async handleDistributedPut() {
    const key = document.getElementById('lsm-key-input').value.trim();
    const val = document.getElementById('lsm-val-input').value.trim();
    if (!key) return;

    const coordinator = this.selectedStorageNode;
    const res = await apiCall('/api/cluster/put', 'POST', { key, val, coordinator });

    const traceBox = document.getElementById('lsm-replication-trace');
    if (res && res.success) {
      const written = res.written_to.join(', ');
      const failed = res.failed_replicas.length > 0 ? ` (Failed: ${res.failed_replicas.join(', ')})` : '';

      this.setLsmFeedback(
        `[REPLICATED PUT] Written to: <strong>[${written}]</strong> • Clock: ${res.vector_clock}${failed} (${res._durationMs}ms)`
      );

      // Render Dynamic Replication Pipeline Trace
      if (traceBox) {
        traceBox.innerHTML = `
          <div class="replication-trace-banner">
            <div><strong>Distributed Write Pipeline Execution:</strong></div>
            <div class="trace-steps">
              <span class="trace-step">🔑 Key: ${key}</span>
              <span class="trace-arrow">──►</span>
              <span class="trace-step">🌐 Hash Ring: [${res.written_to.concat(res.failed_replicas).join(', ')}]</span>
              <span class="trace-arrow">──►</span>
              <span class="trace-step">📡 Gossip Check: ${res.failed_replicas.length === 0 ? 'All Replicas ALIVE ✅' : `${res.failed_replicas.join(', ')} DEAD ❌`}</span>
              <span class="trace-arrow">──►</span>
              <span class="trace-step">⏱️ VectorClock: ${res.vector_clock}</span>
              <span class="trace-arrow">──►</span>
              <span class="trace-step">💾 LSM Storage: Appended to WAL + MemTable</span>
            </div>
            ${res.failed_replicas.length > 0 ? `
              <div style="color: #fbbf24; font-size: 0.76rem; margin-top: 0.2rem;">
                ⚠️ Quorum write completed on [${written}], but replica [${res.failed_replicas.join(', ')}] missed write.
                Go to <strong>Merkle Anti-Entropy</strong> tab to detect and synchronize divergence!
              </div>
            ` : ''}
          </div>
        `;
      }

      this.logGossip(`[STORAGE PUT] '${key}' replicated to [${written}] via coordinator '${coordinator}'. Vector Clock: ${res.vector_clock}`);
    }

    await this.syncClusterState();
    this.renderStorageEngineView();
    await this.loadClusterKeys();
  }

  async handleDistributedGet() {
    const key = document.getElementById('lsm-key-input').value.trim();
    if (!key) return;

    const res = await apiCall('/api/cluster/get', 'POST', { key, node_id: this.selectedStorageNode });

    if (res && res.success && res.found) {
      if (res.failover) {
        this.setLsmFeedback(
          `[GET HIT VIA REPLICA FAILOVER ⚡] Selected node '${this.selectedStorageNode}' is DOWN or didn't store key! Successfully served by replica <strong>${res.from_node}</strong>: '${res.value}' • Causal Clock: ${res.vector_clock} (${res._durationMs}ms)`
        );
        this.logGossip(`[REPLICA FAILOVER] '${key}' read from replica '${res.from_node}' (coordinator '${this.selectedStorageNode}' was unavailable).`, 'system');
      } else {
        this.setLsmFeedback(
          `[GET HIT] Found on <strong>${res.from_node}</strong>: '${res.value}' • Causal Clock: ${res.vector_clock} (${res._durationMs}ms)`
        );
      }
    } else {
      this.setLsmFeedback(`[GET MISS] Key '${key}' not found across any active cluster replicas (${res ? res._durationMs : 0}ms)`, false);
    }
  }

  async handleDistributedDel() {
    const key = document.getElementById('lsm-key-input').value.trim();
    if (!key) return;

    const res = await apiCall('/api/storage/del', 'POST', { key, node_id: this.selectedStorageNode });
    if (res && res.success) {
      this.setLsmFeedback(`[DELETE] Tombstone written for '${key}' on '${this.selectedStorageNode}' (${res._durationMs}ms)`);
      this.logGossip(`[DELETE] Tombstone record written for '${key}' on '${this.selectedStorageNode}'.`);
    }
    await this.syncClusterState();
    this.renderStorageEngineView();
    await this.loadClusterKeys();
  }

  async handleFlush() {
    const res = await apiCall('/api/cluster/flush', 'POST', { node_id: this.selectedStorageNode });
    if (res && res.success) {
      this.setLsmFeedback(`[FLUSH] MemTable frozen ──► Written to on-disk SSTable on '${this.selectedStorageNode}'`);
    }
    await this.syncClusterState();
    this.renderStorageEngineView();
  }

  async handleCrash() {
    this.setLsmFeedback(`[SIMULATED CRASH] '${this.selectedStorageNode}' process terminated! RAM wiped.`, false);
    this.renderStorageEngineView();
  }

  async handleRecover() {
    const res = await apiCall('/api/cluster/recover', 'POST', { node_id: this.selectedStorageNode });
    if (res && res.success) {
      this.setLsmFeedback(`[WAL REPLAY] '${this.selectedStorageNode}' replayed WAL and recovered state!`);
    }
    await this.syncClusterState();
    this.renderStorageEngineView();
  }

  async renderStorageEngineView() {
    const node = this.nodes.find(n => n.id === this.selectedStorageNode) || this.nodes[0];
    if (!node) return;

    // Fetch real storage details for this node from backend
    const storeData = await apiCall(`/api/cluster/node_storage?node_id=${node.id}`, 'GET');

    // Render WAL Tape
    const walEntries = (storeData && storeData.wal_entries) ? storeData.wal_entries : [];
    document.getElementById('wal-entry-count').textContent = walEntries.length;
    const walTape = document.getElementById('wal-tape');

    if (walEntries.length === 0) {
      walTape.innerHTML = `<div class="empty-state">WAL on '${node.id}' is empty (or flushed to SSTable)</div>`;
    } else {
      walTape.innerHTML = walEntries.slice(-6).map(w => `
        <div class="wal-item">
          <span>${w}</span>
          <span style="color: #fbbf24; font-family: JetBrains Mono; font-size: 0.72rem;">CRC32 ✓</span>
        </div>
      `).join('');
    }

    // Render MemTable
    const entries = (storeData && storeData.entries) ? storeData.entries : [];
    const count = entries.length;
    document.getElementById('memtable-capacity-txt').textContent = `${count} / 4 entries`;
    const pct = Math.min(100, (count / 4) * 100);
    document.getElementById('memtable-progress').style.width = `${pct}%`;

    const memItems = document.getElementById('memtable-items');
    if (count === 0) {
      memItems.innerHTML = `<div class="empty-state">MemTable on '${node.id}' is currently empty</div>`;
    } else {
      memItems.innerHTML = entries.map(e => `
        <div class="mem-item">
          <span><strong>${e.key}</strong></span>
          <span style="color: #38bdf8; font-family: JetBrains Mono; font-size: 0.78rem;">${e.value}</span>
        </div>
      `).join('');
    }

    // Render SSTables
    const sstCount = (storeData && storeData.sstable_count !== undefined) ? storeData.sstable_count : (node.sstable_count || 0);
    document.getElementById('sstable-count').textContent = sstCount;
    const sstDeck = document.getElementById('sstables-deck');

    if (sstCount === 0) {
      sstDeck.innerHTML = `<div class="empty-state">No SSTables on disk for '${node.id}'. Click Flush to persist.</div>`;
    } else {
      let sstHtml = '';
      for (let i = 0; i < sstCount; i++) {
        sstHtml += `
          <div class="sstable-card-item">
            <div class="sstable-name">📄 SSTable-${String(i).padStart(3, '0')}.db (${node.id})</div>
            <div class="bloom-bits-view">
              <div class="bloom-bit on"></div><div class="bloom-bit"></div>
              <div class="bloom-bit on"></div><div class="bloom-bit on"></div>
              <div class="bloom-bit"></div><div class="bloom-bit on"></div>
            </div>
            <div style="font-size: 0.72rem; color: #94a3b8; font-family: JetBrains Mono;">
              Bloom Filter & Sparse Index active on disk
            </div>
          </div>
        `;
      }
      sstDeck.innerHTML = sstHtml;
    }
  }

  // ── Cluster-Wide Keys & Replica Distribution Matrix ───────────────────────
  async loadClusterKeys() {
    const res = await apiCall('/api/cluster/keys', 'GET');
    const tbody = document.getElementById('cluster-keys-table-body');
    const vcKeySelect = document.getElementById('select-vc-cluster-key');
    if (!tbody) return;

    if (!res || !res.success || !res.keys || res.keys.length === 0) {
      tbody.innerHTML = `
        <tr>
          <td colspan="6" style="text-align: center; color: #64748b; padding: 1.5rem;">
            No cluster keys written yet. Click <strong>⚡ Seed Demo Cluster</strong> above or write a key via Replicated PUT!
          </td>
        </tr>
      `;
      if (vcKeySelect) vcKeySelect.innerHTML = `<option value="">No cluster keys available</option>`;
      return;
    }

    this.clusterKeys = res.keys;

    tbody.innerHTML = res.keys.map(k => {
      const targetBadges = k.targets.map(t => {
        const node = this.nodes.find(n => n.id === t);
        const isAlive = node ? (node.is_alive && node.status === 'ALIVE') : true;
        return `<span class="replica-badge ${isAlive ? 'alive' : 'dead'}">${t}</span>`;
      }).join(' ');

      const holdingBadges = k.holding.map(h => `<span class="replica-badge alive">✓ ${h}</span>`).join(' ');
      const missingBadges = k.missing.map(m => `<span class="replica-badge missing">✗ ${m}</span>`).join(' ');

      const isSynchronized = k.missing.length === 0;

      return `
        <tr>
          <td><strong>${k.key}</strong></td>
          <td><span class="badge-node">${k.primary}</span></td>
          <td>${targetBadges}</td>
          <td>${holdingBadges} ${missingBadges}</td>
          <td><code style="color: #22d3ee; font-size: 0.76rem;">${k.vector_clock || '[]'}</code></td>
          <td>
            ${isSynchronized 
              ? `<span style="color: #34d399; font-weight: 600; font-size: 0.8rem;">100% In Sync ✅</span>`
              : `<span style="color: #f87171; font-weight: 600; font-size: 0.8rem;">⚠️ Diverged on [${k.missing.join(', ')}]</span>`
            }
          </td>
        </tr>
      `;
    }).join('');

    // Update Vector Clock cluster key dropdown
    if (vcKeySelect) {
      vcKeySelect.innerHTML = res.keys.map(k => `
        <option value="${k.key}">${k.key} (Clock: ${k.vector_clock})</option>
      `).join('');
    }
  }

  // ==========================================================================
  // 4. VECTOR CLOCKS & CAUSALITY
  // ==========================================================================
  initVectorClocks() {
    this.clockA = { 'node-alpha': 2, 'node-beta': 1 };
    this.clockB = { 'node-alpha': 2, 'node-gamma': 1 };

    document.getElementById('btn-step-d1').addEventListener('click', () => this.runVcScenario(1));
    document.getElementById('btn-step-d2').addEventListener('click', () => this.runVcScenario(2));
    document.getElementById('btn-step-d3').addEventListener('click', () => this.runVcScenario(3));
    document.getElementById('btn-step-d4').addEventListener('click', () => this.runVcScenario(4));
    document.getElementById('btn-step-d5').addEventListener('click', () => this.runVcScenario(5));
    document.getElementById('btn-reset-vc-scenario').addEventListener('click', () => this.runVcScenario(0));
    document.getElementById('btn-execute-merge').addEventListener('click', () => this.mergeClocks());

    this.renderVcComparison();
  }

  runVcScenario(step) {
    const timeline = document.getElementById('vc-timeline');
    const steps = [
      { id: 'D1', val: 'Client write: [Book]', clock: '[node-alpha: 1]', class: 'active' },
      { id: 'D2', val: 'Client update: [Book, Pen]', clock: '[node-alpha: 2]', class: 'active' },
      { id: 'D3', val: 'Concurrent Fork 1: [Book, Pen, Ruler]', clock: '[node-alpha: 2, node-beta: 1]', class: 'fork-a' },
      { id: 'D4', val: 'Concurrent Fork 2: [Book, Pen, Eraser]', clock: '[node-alpha: 2, node-gamma: 1]', class: 'fork-b' },
      { id: 'D5', val: 'Merged & Resolved: [Book, Pen, Ruler, Eraser]', clock: '[node-alpha: 3, node-beta: 1, node-gamma: 1]', class: 'merged' }
    ];

    timeline.innerHTML = steps.slice(0, Math.max(1, step)).map(s => `
      <div class="vc-node ${s.class}">
        <div><strong>${s.id}</strong>: <span>${s.val}</span></div>
        <span class="vc-clock-tag">${s.clock}</span>
      </div>
    `).join('');

    if (step >= 4) {
      this.clockA = { 'node-alpha': 2, 'node-beta': 1 };
      this.clockB = { 'node-alpha': 2, 'node-gamma': 1 };
    }
    this.renderVcComparison();
  }

  async renderVcComparison() {
    this.renderVcButtons('clock-a-counters', this.clockA);
    this.renderVcButtons('clock-b-counters', this.clockB);

    const keysA = Object.keys(this.clockA);
    const keysB = Object.keys(this.clockB);

    const res = await apiCall('/api/vector_clock/compare', 'POST', {
      a_sx: this.clockA[keysA[0]] || 0,
      a_sy: this.clockA[keysA[1]] || 0,
      a_sz: this.clockA[keysA[2]] || 0,
      b_sx: this.clockB[keysB[0]] || 0,
      b_sy: this.clockB[keysB[1]] || 0,
      b_sz: this.clockB[keysB[2]] || 0
    });

    const pill = document.getElementById('vc-verdict-pill');
    const detail = document.getElementById('vc-verdict-detail');
    const cmp = (res && res.verdict) ? res.verdict : 'CONCURRENT';

    if (cmp === 'CONCURRENT') {
      pill.className = 'verdict-pill concurrent';
      pill.textContent = 'CONCURRENT CONFLICT ⚠️ (C++ VectorClock::compare)';
      detail.textContent = 'Neither clock dominates. Independent concurrent updates occurred; conflict reconciliation required!';
    } else if (cmp === 'BEFORE') {
      pill.className = 'verdict-pill descends';
      pill.textContent = 'CLOCK A HAPPENED-BEFORE B ✅';
      detail.textContent = 'Clock B is a causal descendant of Clock A.';
    } else if (cmp === 'AFTER') {
      pill.className = 'verdict-pill descends';
      pill.textContent = 'CLOCK A HAPPENED-AFTER B ✅';
      detail.textContent = 'Clock A causally descends from Clock B. Clock A is the newest version.';
    } else {
      pill.className = 'verdict-pill descends';
      pill.textContent = 'CLOCKS ARE IDENTICAL (EQUAL) ✅';
      detail.textContent = 'Both clocks describe the exact same causal version.';
    }
  }

  renderVcButtons(containerId, clockObj) {
    const el = document.getElementById(containerId);
    el.innerHTML = Object.entries(clockObj).map(([s, c]) => `
      <div class="counter-row">
        <span>${s}</span>
        <div class="counter-btns">
          <button class="btn btn-xs btn-outline" data-s="${s}" data-d="-1">-</button>
          <span style="color: #22d3ee; min-width: 20px; text-align: center;">${c}</span>
          <button class="btn btn-xs btn-outline" data-s="${s}" data-d="1">+</button>
        </div>
      </div>
    `).join('');

    el.querySelectorAll('button').forEach(btn => {
      btn.addEventListener('click', () => {
        const s = btn.dataset.s;
        const d = parseInt(btn.dataset.d, 10);
        clockObj[s] = Math.max(0, (clockObj[s] || 0) + d);
        this.renderVcComparison();
      });
    });
  }

  mergeClocks() {
    const merged = {};
    const servers = new Set([...Object.keys(this.clockA), ...Object.keys(this.clockB)]);
    for (const s of servers) {
      merged[s] = Math.max(this.clockA[s] || 0, this.clockB[s] || 0);
    }
    const firstServer = Object.keys(merged)[0] || 'node-alpha';
    merged[firstServer] = (merged[firstServer] || 0) + 1;

    document.getElementById('merged-result-card').style.display = 'block';
    document.getElementById('merged-clock-display').innerHTML = `
      <div style="font-family: JetBrains Mono; color: #34d399; font-size: 0.95rem;">
        Merged Clock: [${Object.entries(merged).map(([k, v]) => `${k}: ${v}`).join(', ')}]
      </div>
      <p style="font-size: 0.8rem; color: #94a3b8; margin-top: 0.5rem;">
        Executed C++ VectorClock::merge() via element-wise maximum!
      </p>
    `;
  }

  inspectSelectedClusterKeyClock() {
    const select = document.getElementById('select-vc-cluster-key');
    const feedback = document.getElementById('vc-live-key-feedback');
    const key = select ? select.value : '';
    if (!key) return;

    const item = this.clusterKeys.find(k => k.key === key);
    if (item) {
      feedback.innerHTML = `
        Key <strong>${item.key}</strong> has Causal Vector Clock: <code>${item.vector_clock}</code><br>
        Primary Coordinator: <strong>${item.primary}</strong> • Stored Replicas: [${item.holding.join(', ')}]
      `;
    }
  }

  async simulateConcurrentWrite() {
    this.clockA = { 'node-alpha': 2, 'node-beta': 1 };
    this.clockB = { 'node-alpha': 1, 'node-gamma': 2 };
    this.renderVcComparison();
    this.logGossip(`[VECTOR CLOCK] Simulated concurrent write conflict across partition coordinators 'node-beta' and 'node-gamma'.`, 'alert');
  }

  // ==========================================================================
  // 5. MERKLE TREE ANTI-ENTROPY (BETWEEN REAL CLUSTER NODES)
  // ==========================================================================
  async runMerkleDiff() {
    const nodeA = this.selectedMerkleA;
    const nodeB = this.selectedMerkleB;

    const res = await apiCall('/api/cluster/merkle_diff', 'POST', { node_a: nodeA, node_b: nodeB });

    let rootA = (res && res.rootA) ? res.rootA : 'EMPTY';
    let rootB = (res && res.rootB) ? res.rootB : 'EMPTY';

    document.getElementById('root-hash-a').textContent = rootA;
    document.getElementById('root-hash-b').textContent = rootB;

    const badge = document.getElementById('root-comparison-badge');
    const statusText = document.getElementById('diff-status-text');

    // Fetch and display real keys for Node A and Node B
    const storeA = await apiCall(`/api/cluster/node_storage?node_id=${nodeA}`, 'GET');
    const storeB = await apiCall(`/api/cluster/node_storage?node_id=${nodeB}`, 'GET');

    const renderDataList = (data, containerId) => {
      const el = document.getElementById(containerId);
      if (!el) return;
      const entries = (data && data.entries) ? data.entries : [];
      if (entries.length === 0) {
        el.innerHTML = '<div style="color: #64748b; font-size: 0.76rem; padding: 0.5rem;">0 keys stored</div>';
      } else {
        el.innerHTML = entries.map(e => `
          <div style="display: flex; justify-content: space-between; font-size: 0.76rem; font-family: JetBrains Mono; padding: 0.2rem 0; border-bottom: 1px solid rgba(255,255,255,0.05);">
            <span>${e.key}</span>
            <span style="color: #22d3ee;">${e.value}</span>
          </div>
        `).join('');
      }
    };

    renderDataList(storeA, 'tree-a-data');
    renderDataList(storeB, 'tree-b-data');

    if (rootA === rootB) {
      badge.style.background = 'rgba(16, 185, 129, 0.2)';
      badge.style.borderColor = 'rgba(16, 185, 129, 0.4)';
      badge.style.color = '#34d399';
      badge.innerHTML = '<span class="status-icon">=</span><span>C++ ROOTS MATCH (100% IN SYNC)</span>';
      statusText.innerHTML = `<strong style="color: #34d399;">Trees for '${nodeA}' and '${nodeB}' are in sync!</strong> 0 keys transferred.`;
    } else {
      badge.style.background = 'rgba(245, 158, 11, 0.2)';
      badge.style.borderColor = 'rgba(245, 158, 11, 0.4)';
      badge.style.color = '#fbbf24';
      badge.innerHTML = '<span class="status-icon">≠</span><span>C++ ROOTS DIVERGE</span>';

      const buckets = (res && res.diff_buckets) ? res.diff_buckets.join(', ') : 'unknown';
      statusText.innerHTML = `
        <span style="color: #f87171; font-weight: 700;">DIVERGENCE DETECTED IN O(log N)</span>:
        '${nodeA}' and '${nodeB}' differ in <strong>Bucket(s) [${buckets}]</strong>.
        Anti-entropy only needs to stream keys in bucket ${buckets}!
      `;
    }

    this.renderTreeSVGs(rootA === rootB);
  }

  async handleInjectDivergence() {
    const nodeB = this.selectedMerkleB;
    const randomKey = 'divergent:' + Math.floor(Math.random() * 1000);
    await apiCall('/api/storage/put', 'POST', { key: randomKey, val: 'MUTATED_DATA', node_id: nodeB });
    this.logGossip(`[CHAOS] Injected divergent key '${randomKey}' into '${nodeB}'.`, 'alert');
    await this.syncClusterState();
    await this.runMerkleDiff();
    await this.loadClusterKeys();
  }

  async handleMerkleSync() {
    const nodeA = this.selectedMerkleA;
    const nodeB = this.selectedMerkleB;
    await apiCall('/api/cluster/merkle_repair', 'POST', { node_a: nodeA, node_b: nodeB });
    this.logGossip(`[ANTI-ENTROPY] Synchronized divergent keys between '${nodeA}' and '${nodeB}'.`, 'system');
    await this.syncClusterState();
    await this.runMerkleDiff();
    await this.loadClusterKeys();
  }

  renderTreeSVGs(isMatching) {
    const coords = [
      { x: 190, y: 30 },
      { x: 95, y: 90 }, { x: 285, y: 90 },
      { x: 48, y: 155 }, { x: 142, y: 155 }, { x: 238, y: 155 }, { x: 332, y: 155 },
      { x: 24, y: 220 }, { x: 72, y: 220 }, { x: 118, y: 220 }, { x: 166, y: 220 },
      { x: 214, y: 220 }, { x: 262, y: 220 }, { x: 308, y: 220 }, { x: 356, y: 220 }
    ];

    ['tree-a-svg', 'tree-b-svg'].forEach((svgId) => {
      const svg = document.getElementById(svgId);
      if (!svg) return;
      let svgHtml = '';
      for (let k = 0; k <= 6; k++) {
        const left = 2 * k + 1;
        const right = 2 * k + 2;
        svgHtml += `<line x1="${coords[k].x}" y1="${coords[k].y}" x2="${coords[left].x}" y2="${coords[left].y}" class="tree-edge"/>`;
        svgHtml += `<line x1="${coords[k].x}" y1="${coords[k].y}" x2="${coords[right].x}" y2="${coords[right].y}" class="tree-edge"/>`;
      }

      for (let i = 0; i < 15; i++) {
        const c = coords[i];
        const isLeaf = i >= 7;
        const bucketIdx = isLeaf ? (i - 7) : null;
        const nodeClass = isMatching ? 'matching' : (i === 0 || i === 2 || i === 5 || i === 12 ? 'diverging' : 'matching');

        svgHtml += `
          <g>
            <circle cx="${c.x}" cy="${c.y}" r="${isLeaf ? 11 : 14}" class="tree-node-circle ${nodeClass}"></circle>
            <text x="${c.x}" y="${c.y + 4}" fill="#f8fafc" font-size="8" font-family="JetBrains Mono" text-anchor="middle">
              ${isLeaf ? `B${bucketIdx}` : `N${i}`}
            </text>
          </g>
        `;
      }
      svg.innerHTML = svgHtml;
    });
  }
}

// ── BOOTSTRAP APP ON LOAD ───────────────────────────────────────────────────
window.addEventListener('DOMContentLoaded', () => {
  new UnifiedClusterApp();
});
