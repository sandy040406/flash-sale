// Configuration
// Same-origin relative URLs: works behind NGINX (:80) and when served directly by uvicorn (:8000)
const API_BASE = '';

const PRODUCT_ID = 1;

// Click-lock and drawer state (read by the handlers below)
let isSubmitting = false;
let currentStock = 500;
let drawerPollInterval = null;

// DOM Elements
const buyButton = document.getElementById('buyButton');
const buyBtnText = document.getElementById('buyBtnText');
const btnSpinner = document.getElementById('btnSpinner');
const userIdInput = document.getElementById('userIdInput');
const randomUserBtn = document.getElementById('randomUserBtn');

const stockCount = document.getElementById('stockCount');
const unitWall = document.getElementById('unitWall');
const countdownTimer = document.getElementById('countdownTimer');
const engineStatus = document.getElementById('engineStatus');

const ticketCard = document.getElementById('ticketCard');
const ticketIdDisplay = document.getElementById('ticketId');
const queueIndicator = document.getElementById('queueIndicator');
const queueStatusTitle = document.getElementById('queueStatusTitle');
const queueStatusMessage = document.getElementById('queueStatusMessage');
const stepRedis = document.getElementById('stepRedis');
const stepStream = document.getElementById('stepStream');
const stepDb = document.getElementById('stepDb');
const protocolModeSelect = document.getElementById('protocolModeSelect');

// Drawer Elements
const auditDrawer = document.getElementById('auditDrawer');
const auditToggleBtn = document.getElementById('auditToggleBtn');
const closeDrawerBtn = document.getElementById('closeDrawerBtn');
const auditInitial = document.getElementById('auditInitial');
const auditRedis = document.getElementById('auditRedis');
const auditInTransit = document.getElementById('auditInTransit');
const auditOrders = document.getElementById('auditOrders');
const auditOversold = document.getElementById('auditOversold');
const consistencyBanner = document.getElementById('consistencyBanner');
const simulateRushBtn = document.getElementById('simulateRushBtn');
const resetSaleBtn = document.getElementById('resetSaleBtn');

// ============================================================================
// Unit wall: one cell per physical phone, claimed from the top-left as stock falls.
// ============================================================================
const TOTAL_UNITS = 500;
const wallCells = [];
let renderedStock = TOTAL_UNITS;

function buildWall() {
  const frag = document.createDocumentFragment();
  for (let i = 0; i < TOTAL_UNITS; i++) {
    const cell = document.createElement('i');
    wallCells.push(cell);
    frag.appendChild(cell);
  }
  unitWall.appendChild(frag);
}

function setStock(n) {
  n = Math.max(0, Math.min(TOTAL_UNITS, Number(n) || 0));
  currentStock = n;
  stockCount.textContent = n;
  unitWall.setAttribute('aria-label', `${n} of ${TOTAL_UNITS} units remaining`);
  // Cell i is claimed once i < sold; only touch the cells that changed.
  const sold = TOTAL_UNITS - n;
  const oldSold = TOTAL_UNITS - renderedStock;
  const lo = Math.min(sold, oldSold);
  const hi = Math.max(sold, oldSold);
  const animate = hi - lo <= 10;
  for (let i = lo; i < hi; i++) {
    const cell = wallCells[i];
    const claimed = i < sold;
    cell.classList.toggle('is-claimed', claimed);
    if (claimed && animate) {
      cell.classList.add('just');
      setTimeout(() => cell.classList.remove('just'), 1000);
    }
  }
  renderedStock = n;
}

// Mark the unit this browser just claimed so a single claim is easy to spot.
function markMine() {
  clearMine();
  const cell = wallCells[TOTAL_UNITS - currentStock - 1];
  if (cell) {
    cell.classList.add('is-mine');
    document.getElementById('mineKey').hidden = false;
  }
}

function clearMine() {
  unitWall.querySelectorAll('.is-mine').forEach(c => c.classList.remove('is-mine'));
  document.getElementById('mineKey').hidden = true;
}

// ============================================================================
// Phase A.3: Optimized Local State Manager
// Handles real-time queue states, optimistic updates, and protocol selection.
// ============================================================================
class FlashSaleStateManager {
  constructor() {
    this.state = {
      isSubmitting: false,
      currentStock: 500,
      protocolMode: protocolModeSelect ? protocolModeSelect.value : 'sse',
      ticketId: null,
      status: 'IDLE', // IDLE, RESERVING, QUEUED, PROCESSING, CONFIRMED, FAILED
      activeEventSource: null,
      pollingInterval: null,
      drawerPollInterval: null
    };
    this.listeners = [];
  }

  get(key) {
    return this.state[key];
  }

  set(partial) {
    this.state = { ...this.state, ...partial };
    this.listeners.forEach(fn => fn(this.state));
  }

  subscribe(listener) {
    this.listeners.push(listener);
  }
}

const stateManager = new FlashSaleStateManager();

// Sync dropdown mode
if (protocolModeSelect) {
  protocolModeSelect.addEventListener('change', (e) => {
    stateManager.set({ protocolMode: e.target.value });
    console.log(`[StateManager] Real-time protocol switched to: ${e.target.value.toUpperCase()}`);
    // If ticket is currently being tracked, re-route
    const activeTicket = stateManager.get('ticketId');
    const currentStatus = stateManager.get('status');
    if (activeTicket && (currentStatus === 'QUEUED' || currentStatus === 'PROCESSING')) {
      trackTicketStatus(activeTicket);
    }
  });
}

// 1. Initialization
document.addEventListener('DOMContentLoaded', () => {
  buildWall();
  fetchProductStatus();
  fetchAuditStats();
  startStockPolling();
  startCountdownTimer();
  generateRandomUserId();
});

// 2. Fetch Product Status & Update Stock Meter
async function fetchProductStatus() {
  try {
    const res = await fetch(`${API_BASE}/api/v1/products/${PRODUCT_ID}`);
    if (!res.ok) throw new Error('API unreachable');
    const data = await res.json();
    
    stateManager.set({ currentStock: data.remaining_stock });
    setStock(data.remaining_stock);

    if (data.remaining_stock <= 0) {
      buyButton.disabled = true;
      buyBtnText.textContent = "Sold out";
    }
    engineStatus.textContent = "Live";
    engineStatus.dataset.state = "live";
  } catch (err) {
    engineStatus.textContent = "Offline";
    engineStatus.dataset.state = "offline";
    console.warn('API error:', err);
  }
}

// 3. Periodic Stock Polling (Dynamic Polling)
function startStockPolling() {
  setInterval(fetchProductStatus, 3000);
}

// 4. Client-Side Click Debounce & Lock Engine
buyButton.addEventListener('click', async () => {
  // Prevent repetitive API spamming
  if (isSubmitting || currentStock <= 0) return;
  
  const userId = userIdInput.value.trim();
  if (!userId) {
    userIdInput.classList.add('invalid');
    userIdInput.focus();
    return;
  }

  userIdInput.classList.remove('invalid');

  // 1. Instant UI Lock (First Click Lock)
  isSubmitting = true;
  buyButton.disabled = true;
  btnSpinner.style.display = 'inline-block';
  buyBtnText.textContent = "Reserving your unit";

  // 2. Optimistic UI Display
  showTicketCard("tok_requesting...", "QUEUED", "Reserving your unit", "Checking stock in memory.");
  resetTicketSteps();
  stepRedis.classList.add('step-done');

  try {
    const response = await fetch(`${API_BASE}/api/v1/checkout`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        user_id: userId,
        product_id: PRODUCT_ID,
        quantity: 1
      })
    });

    const result = await response.json();

    if (response.status === 202) {
      // Token Granted! Handed off to Redis Stream
      const ticketId = result.ticket_id;
      ticketIdDisplay.textContent = ticketId;
      stepStream.classList.add('step-done');
      
      updateTicketUI("PROCESSING", "Unit reserved", "Your order is in the queue and is being saved.");
      
      // Start real-time tracking (SSE with polling fallback)
      trackTicketStatus(ticketId);

      // Instantly decrement local stock optimistically
      setStock(currentStock - 1);
      markMine();
    } 
    else if (response.status === 410) {
      // Sold Out
      updateTicketUI("FAILED", "Sold out", "All 500 units have been claimed.");
      buyBtnText.textContent = "Sold out";
      buyButton.disabled = true;
      isSubmitting = false;
      btnSpinner.style.display = 'none';
    } 
    else if (response.status === 409) {
      // Duplicate order
      updateTicketUI("FAILED", "Already claimed", "This customer ID already has a unit. Use a different ID to claim another.");
      buyBtnText.textContent = "Already claimed";
      buyButton.disabled = false;
      isSubmitting = false;
      btnSpinner.style.display = 'none';
    }
    else {
      // 429 (rate limited), 5xx, etc: release the lock so the user is never stuck
      updateTicketUI("FAILED", response.status === 429 ? "Too many requests" : "Checkout unavailable", response.status === 429 ? "Slow down and try again in a few seconds." : "Try again in a moment.");
      buyBtnText.textContent = "Claim your phone";
      buyButton.disabled = false;
      isSubmitting = false;
      btnSpinner.style.display = 'none';
    }
  } catch (err) {
    updateTicketUI("FAILED", "Connection lost", "Check your network and try again.");
    buyButton.disabled = false;
    isSubmitting = false;
    btnSpinner.style.display = 'none';
  }
});

// 5. Real-Time Ticket Status: Protocol-Aware Tracking (SSE with Dynamic Polling Fallback)
function trackTicketStatus(ticketId) {
  // Clear any previous streams or timers
  if (stateManager.get('activeEventSource')) {
    stateManager.get('activeEventSource').close();
    stateManager.set({ activeEventSource: null });
  }
  if (stateManager.get('pollingInterval')) {
    clearInterval(stateManager.get('pollingInterval'));
    stateManager.set({ pollingInterval: null });
  }

  stateManager.set({ ticketId, status: 'PROCESSING' });

  const mode = stateManager.get('protocolMode');
  console.log(`[Tracking] Tracking ticket ${ticketId} using protocol: ${mode.toUpperCase()}`);

  if (mode === 'polling') {
    startTicketStatusPolling(ticketId);
    return;
  }

  // SSE Stream Mode (Preferred)
  if (window.EventSource) {
    try {
      const es = new EventSource(`${API_BASE}/api/v1/checkout/sse/${ticketId}`);
      stateManager.set({ activeEventSource: es });

      es.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          handleTicketStatusUpdate(data);

          if (data.status === 'CONFIRMED' || data.status === 'FAILED') {
            es.close();
            stateManager.set({ activeEventSource: null });
          }
        } catch (e) {
          console.error("SSE parse error:", e);
        }
      };

      es.onerror = () => {
        console.warn("SSE connection interrupted. Falling back to dynamic polling...");
        es.close();
        stateManager.set({ activeEventSource: null });
        startTicketStatusPolling(ticketId);
      };
      return;
    } catch (e) {
      console.warn("SSE initialization failed, falling back to dynamic polling:", e);
    }
  }

  // Fallback to Dynamic Polling
  startTicketStatusPolling(ticketId);
}

function handleTicketStatusUpdate(data) {
  if (data.status === 'CONFIRMED') {
    const timer = stateManager.get('pollingInterval');
    if (timer) clearInterval(timer);
    stateManager.set({ status: 'CONFIRMED', pollingInterval: null });

    stepDb.classList.add('step-done');
    updateTicketUI(
      "CONFIRMED", 
      `Order confirmed${data.order_id ? ' (#' + data.order_id + ')' : ''}`,
      "Your phone is secured and the order is saved."
    );
    buyBtnText.textContent = "Claimed";
    btnSpinner.style.display = 'none';
    fetchAuditStats();
  } else if (data.status === 'FAILED') {
    const timer = stateManager.get('pollingInterval');
    if (timer) clearInterval(timer);
    stateManager.set({ status: 'FAILED', isSubmitting: false, pollingInterval: null });

    updateTicketUI("FAILED", "Order failed", data.message);
    buyBtnText.textContent = "Claim your phone";
    buyButton.disabled = false;
    isSubmitting = false;
    btnSpinner.style.display = 'none';
    // Roll back the optimistic stock drop and our marked tile
    clearMine();
    fetchProductStatus();
  }
}

function startTicketStatusPolling(ticketId) {
  let timer = stateManager.get('pollingInterval');
  if (timer) clearInterval(timer);

  let attempts = 0;
  timer = setInterval(async () => {
    attempts++;
    try {
      const res = await fetch(`${API_BASE}/api/v1/checkout/status/${ticketId}`);
      if (!res.ok) return;

      const data = await res.json();
      handleTicketStatusUpdate(data);

      if (data.status === 'CONFIRMED' || data.status === 'FAILED' || attempts > 30) {
        clearInterval(timer);
        stateManager.set({ pollingInterval: null });
      }
    } catch (e) {
      console.warn("Polling error:", e);
    }
  }, 400);

  stateManager.set({ pollingInterval: timer });
}

// 6. UI Helpers
function showTicketCard(id, status, title, message) {
  ticketCard.style.display = 'block';
  ticketIdDisplay.textContent = id;
  updateTicketUI(status, title, message);
}

function updateTicketUI(status, title, message) {
  queueStatusTitle.textContent = title;
  queueStatusMessage.textContent = message;
  queueIndicator.className = 'queue-indicator';

  if (status === 'CONFIRMED') {
    queueIndicator.classList.add('confirmed');
  } else if (status === 'FAILED') {
    queueIndicator.classList.add('failed');
  }
}

function resetTicketSteps() {
  stepRedis.className = 'step-item';
  stepStream.className = 'step-item';
  stepDb.className = 'step-item';
}

function generateRandomUserId() {
  const rand = Math.floor(1000 + Math.random() * 9000);
  userIdInput.value = `usr_fest_${rand}`;
}

randomUserBtn.addEventListener('click', () => {
  generateRandomUserId();
  buyButton.disabled = false;
  isSubmitting = false;
  buyBtnText.textContent = "Claim your phone";
  ticketCard.style.display = 'none';
  btnSpinner.style.display = 'none';
  clearMine();
});

// 7. Countdown Timer (Visual Excitement)
function startCountdownTimer() {
  let seconds = 15 * 60 - 1;
  setInterval(() => {
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    countdownTimer.textContent = `${String(mins).padStart(2, '0')}:${String(secs).padStart(2, '0')}`;
    if (seconds > 0) seconds--;
  }, 1000);
}

// 8. Evaluator Drawer & Audit Lab
// 8. Evaluator Drawer & Audit Lab
auditToggleBtn.addEventListener('click', () => {
  auditDrawer.classList.add('open');
  document.documentElement.classList.add('drawer-open');
  fetchAuditStats();
  if (drawerPollInterval) clearInterval(drawerPollInterval);
  drawerPollInterval = setInterval(fetchAuditStats, 750);
});

closeDrawerBtn.addEventListener('click', () => {
  auditDrawer.classList.remove('open');
  document.documentElement.classList.remove('drawer-open');
  if (drawerPollInterval) clearInterval(drawerPollInterval);
});

async function fetchAuditStats() {
  try {
    const res = await fetch(`${API_BASE}/api/v1/admin/stats`);
    if (!res.ok) return;
    const stats = await res.json();
    updateAuditStatsUI(stats);
  } catch (err) {
    console.error('Failed to fetch audit stats:', err);
  }
}

function updateAuditStatsUI(stats) {
  auditInitial.textContent = stats.initial_stock;
  auditRedis.textContent = stats.redis_remaining_stock;
  if (auditInTransit) auditInTransit.textContent = stats.in_transit_queue_count || 0;
  auditOrders.textContent = stats.database_confirmed_orders;
  auditOversold.textContent = stats.oversold_count;

  let state, text;
  if (stats.oversold_count > 0) {
    state = 'bad';
    text = `Oversold by ${stats.oversold_count}. Stock accounting is broken.`;
  } else if (stats.is_perfectly_consistent && stats.database_confirmed_orders > 0) {
    state = 'ok';
    text = `Consistent. ${stats.database_confirmed_orders} saved, ${stats.redis_remaining_stock} left, 0 oversold.`;
  } else if ((stats.in_transit_queue_count || 0) > 0) {
    state = 'busy';
    text = `Saving ${stats.in_transit_queue_count} queued orders to the database.`;
  } else if (stats.is_perfectly_consistent) {
    state = 'ok';
    text = 'Consistent. No orders yet.';
  } else {
    state = 'idle';
    text = `Sale in progress, ${stats.redis_remaining_stock} units available.`;
  }
  consistencyBanner.dataset.state = state;
  consistencyBanner.querySelector('p').textContent = text;
}

// 9. Reset Flash Sale Button
resetSaleBtn.addEventListener('click', async () => {
  if (!confirm("Reset flash sale back to 500 items?")) return;
  resetSaleBtn.disabled = true;
  resetSaleBtn.textContent = "Resetting";
  try {
    const res = await fetch(`${API_BASE}/api/v1/admin/reset`, { method: 'POST' });
    if (!res.ok) {
      resetSaleBtn.textContent = res.status === 403 ? "Reset needs demo mode or admin token" : "Reset failed";
      setTimeout(() => { resetSaleBtn.disabled = false; resetSaleBtn.textContent = "Reset to 500 units"; }, 3500);
      return;
    }
    await fetchProductStatus();
    await fetchAuditStats();
    generateRandomUserId();
    ticketCard.style.display = 'none';
    buyButton.disabled = false;
    isSubmitting = false;
    clearMine();
    buyBtnText.textContent = "Claim your phone";
  } catch (e) {
    resetSaleBtn.textContent = "Reset failed";
    setTimeout(() => { resetSaleBtn.textContent = "Reset to 500 units"; }, 3500);
  } finally {
    if (resetSaleBtn.textContent === "Resetting") {
      resetSaleBtn.disabled = false;
      resetSaleBtn.textContent = "Reset to 500 units";
    }
  }
});

// 10. Simulate 500-User Surge Button
// Uses the demo-only endpoint (no per-IP limit, needs DEMO_MODE=1 on the server) so one
// browser can stand in for 500 distinct users. The real /checkout stays rate-limited.
simulateRushBtn.addEventListener('click', async () => {
  simulateRushBtn.disabled = true;
  const idleLabel = "Simulate 500 buyers";
  simulateRushBtn.textContent = "Sending 500 requests";

  const outcome = { accepted: 0, soldOut: 0, limited: 0, demoOff: 0, errors: 0 };
  const runId = Math.random().toString(36).substring(2, 9);
  const requests = [];
  for (let i = 0; i < 500; i++) {
    requests.push(
      fetch(`${API_BASE}/api/v1/demo/checkout`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ user_id: `sim_rush_${runId}_${i}`, product_id: PRODUCT_ID, quantity: 1 })
      }).then(r => {
        if (r.status === 202) outcome.accepted++;
        else if (r.status === 410) outcome.soldOut++;
        else if (r.status === 429) outcome.limited++;
        else if (r.status === 404) outcome.demoOff++;
        else outcome.errors++;
      }).catch(() => { outcome.errors++; })
    );
  }
  await Promise.all(requests);
  fetchProductStatus();

  if (outcome.demoOff > 0) {
    simulateRushBtn.textContent = "Demo mode is off (start server with DEMO_MODE=1)";
    setTimeout(() => { simulateRushBtn.disabled = false; simulateRushBtn.textContent = idleLabel; }, 4000);
    return;
  }

  simulateRushBtn.textContent =
    `${outcome.accepted} accepted · ${outcome.soldOut} sold out · ${outcome.limited} rate-limited · saving orders`;

  // Poll until queue is completely settled
  let polls = 0;
  const pollTimer = setInterval(async () => {
    polls++;
    await fetchProductStatus();
    const res = await fetch(`${API_BASE}/api/v1/admin/stats`);
    if (res.ok) {
      const stats = await res.json();
      updateAuditStatsUI(stats);
      simulateRushBtn.textContent = `${outcome.accepted} accepted · ${stats.database_confirmed_orders}/500 saved`;
      
      if (stats.is_perfectly_consistent || (stats.in_transit_queue_count === 0 && stats.database_confirmed_orders === 500) || polls > 25) {
        clearInterval(pollTimer);
        simulateRushBtn.disabled = false;
        simulateRushBtn.textContent =
          `Done: ${outcome.accepted} accepted · ${outcome.soldOut} sold out · ${outcome.limited} rate-limited · ${outcome.errors} errors`;
      }
    }
  }, 250);
});

// Close the audit drawer with Escape; clear the invalid state as the user types
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && auditDrawer.classList.contains('open')) closeDrawerBtn.click();
});
userIdInput.addEventListener('input', () => userIdInput.classList.remove('invalid'));
