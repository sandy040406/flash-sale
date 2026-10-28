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
const stockBar = document.getElementById('stockBar');
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
    
    currentStock = data.remaining_stock;
    stateManager.set({ currentStock: data.remaining_stock });
    stockCount.textContent = data.remaining_stock;
    const percentage = Math.max(0, (data.remaining_stock / 500) * 100);
    stockBar.style.width = `${percentage}%`;

    if (data.remaining_stock <= 0) {
      buyButton.disabled = true;
      buyBtnText.textContent = "SOLD OUT (ALL 500 CLAIMED)";
    }
    engineStatus.textContent = "ONLINE (REDIS LUA ACTIVE)";
  } catch (err) {
    engineStatus.textContent = "OFFLINE";
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
    alert("Please provide a user ID.");
    return;
  }

  // 1. Instant UI Lock (First Click Lock)
  isSubmitting = true;
  buyButton.disabled = true;
  btnSpinner.style.display = 'inline-block';
  buyBtnText.textContent = "RESERVING TOKEN...";

  // 2. Optimistic UI Display
  showTicketCard("tok_requesting...", "QUEUED", "Acquiring Redis Atomic Token...", "Executing atomic Lua decrement in Redis memory...");
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
      
      updateTicketUI("PROCESSING", "Token Acquired! In Processing Queue", "Asynchronous Redis Stream worker persisting transaction to database...");
      
      // Start real-time tracking (SSE with polling fallback)
      trackTicketStatus(ticketId);

      // Instantly decrement local stock optimistically
      currentStock = Math.max(0, currentStock - 1);
      stockCount.textContent = currentStock;
      stockBar.style.width = `${(currentStock / 500) * 100}%`;
    } 
    else if (response.status === 410) {
      // Sold Out
      updateTicketUI("FAILED", "Sale Ended — Sold Out", result.message);
      buyBtnText.textContent = "SOLD OUT";
      buyButton.disabled = true;
      isSubmitting = false;
      btnSpinner.style.display = 'none';
    } 
    else if (response.status === 409) {
      // Duplicate order
      updateTicketUI("FAILED", "Order Limit Reached", result.message);
      buyBtnText.textContent = "ALREADY ORDERED";
      buyButton.disabled = false;
      isSubmitting = false;
      btnSpinner.style.display = 'none';
    }
    else {
      // 429 (rate limited), 5xx, etc: release the lock so the user is never stuck
      updateTicketUI("FAILED", response.status === 429 ? "Too Many Requests" : "Checkout Unavailable", result.message || "Please try again in a moment.");
      buyBtnText.textContent = "CLAIM SMARTPHONE NOW";
      buyButton.disabled = false;
      isSubmitting = false;
      btnSpinner.style.display = 'none';
    }
  } catch (err) {
    updateTicketUI("FAILED", "Network Connection Error", "Could not reach flash sale cluster.");
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
      `🎉 Order Confirmed! (Order #${data.order_id || 'OK'})`, 
      "Smartphone inventory secured with ACID database persistence."
    );
    buyBtnText.textContent = "CLAIMED SUCCESSFULLY";
    btnSpinner.style.display = 'none';
    fetchAuditStats();
  } else if (data.status === 'FAILED') {
    const timer = stateManager.get('pollingInterval');
    if (timer) clearInterval(timer);
    stateManager.set({ status: 'FAILED', isSubmitting: false, pollingInterval: null });

    updateTicketUI("FAILED", "Order Failed", data.message);
    buyBtnText.textContent = "CLAIM SMARTPHONE NOW";
    buyButton.disabled = false;
    btnSpinner.style.display = 'none';
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
  buyBtnText.textContent = "CLAIM SMARTPHONE NOW";
  ticketCard.style.display = 'none';
  btnSpinner.style.display = 'none';
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
  fetchAuditStats();
  if (drawerPollInterval) clearInterval(drawerPollInterval);
  drawerPollInterval = setInterval(fetchAuditStats, 750);
});

closeDrawerBtn.addEventListener('click', () => {
  auditDrawer.classList.remove('open');
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

  if (stats.is_perfectly_consistent) {
    consistencyBanner.innerHTML = `<span class="icon">✅</span><span class="text">System State: <strong>PERFECTLY CONSISTENT</strong> (0 Discrepancy, 500 Settled)</span>`;
    consistencyBanner.style.background = 'rgba(16, 185, 129, 0.15)';
    consistencyBanner.style.borderColor = 'rgba(16, 185, 129, 0.4)';
    consistencyBanner.style.color = '#a7f3d0';
  } else if ((stats.in_transit_queue_count || 0) > 0) {
    consistencyBanner.innerHTML = `<span class="icon">⚡</span><span class="text">Asynchronous Queue Active: Worker Persisting <strong>${stats.in_transit_queue_count}</strong> to DB...</span>`;
    consistencyBanner.style.background = 'rgba(245, 158, 11, 0.15)';
    consistencyBanner.style.borderColor = 'rgba(245, 158, 11, 0.4)';
    consistencyBanner.style.color = '#fde68a';
  } else {
    consistencyBanner.innerHTML = `<span class="icon">ℹ️</span><span class="text">Sale in Progress (${stats.redis_remaining_stock} available)</span>`;
    consistencyBanner.style.background = 'rgba(56, 189, 248, 0.15)';
    consistencyBanner.style.borderColor = 'rgba(56, 189, 248, 0.4)';
    consistencyBanner.style.color = '#bae6fd';
  }
}

// 9. Reset Flash Sale Button
resetSaleBtn.addEventListener('click', async () => {
  if (!confirm("Reset flash sale back to 500 items?")) return;
  resetSaleBtn.disabled = true;
  resetSaleBtn.textContent = "Resetting...";
  try {
    const res = await fetch(`${API_BASE}/api/v1/admin/reset`, { method: 'POST' });
    const data = await res.json();
    await fetchProductStatus();
    await fetchAuditStats();
    generateRandomUserId();
    ticketCard.style.display = 'none';
    buyButton.disabled = false;
    isSubmitting = false;
    buyBtnText.textContent = "CLAIM SMARTPHONE NOW";
  } catch (e) {
    alert("Failed to reset sale: " + e);
  } finally {
    resetSaleBtn.disabled = false;
    resetSaleBtn.textContent = "🔄 Reset Flash Sale to 500 Inventory";
  }
});

// 10. Simulate 500-User Surge Button
simulateRushBtn.addEventListener('click', async () => {
  simulateRushBtn.disabled = true;
  simulateRushBtn.textContent = "Firing 500 Concurrent Requests...";
  
  const CHUNK_SIZE = 50;
  for (let chunk = 0; chunk < 10; chunk++) {
    const requests = [];
    for (let i = 0; i < CHUNK_SIZE; i++) {
      const uid = `sim_rush_${Math.random().toString(36).substring(2, 9)}_${chunk * CHUNK_SIZE + i}`;
      requests.push(
        fetch(`${API_BASE}/api/v1/checkout`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ user_id: uid, product_id: PRODUCT_ID, quantity: 1 })
        }).catch(() => null)
      );
    }
    await Promise.all(requests);
    fetchProductStatus();
    fetchAuditStats();
  }

  simulateRushBtn.textContent = "Draining queue & persisting to DB...";

  // Poll until queue is completely settled
  let polls = 0;
  const pollTimer = setInterval(async () => {
    polls++;
    await fetchProductStatus();
    const res = await fetch(`${API_BASE}/api/v1/admin/stats`);
    if (res.ok) {
      const stats = await res.json();
      updateAuditStatsUI(stats);
      simulateRushBtn.textContent = `Persisting (${stats.database_confirmed_orders}/500 Confirmed)...`;
      
      if (stats.is_perfectly_consistent || (stats.in_transit_queue_count === 0 && stats.database_confirmed_orders === 500) || polls > 25) {
        clearInterval(pollTimer);
        simulateRushBtn.disabled = false;
        simulateRushBtn.textContent = "⚡ Trigger Concurrent Spike (500 Requests)";
      }
    }
  }, 250);
});
