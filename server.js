#!/usr/bin/env node
import express from 'express';
import { WebSocketServer } from 'ws';
import http from 'http';
import WebSocket from 'ws';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const PORTS = [9000, 9001, 9002, 9003];
const DISCOVERY_INTERVAL = 10000;
const POLL_INTERVAL = 500; // Fast sync interval (0.5 seconds)

// Application State
let cascades = new Map(); // Map<cascadeId, { id, cdp: { ws, contexts, rootContextId }, metadata, snapshot, snapshotHash }>
let wss = null;

// --- Helpers ---

// Simple hash function
function hashString(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
        const char = str.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash;
    }
    return hash.toString(36);
}

// HTTP GET JSON
function getJson(url) {
    return new Promise((resolve, reject) => {
        const req = http.get(url, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try { resolve(JSON.parse(data)); } catch (e) { resolve([]); } // return empty on parse error
            });
        });
        req.on('error', () => resolve([])); // return empty on network error
        req.setTimeout(2000, () => {
            req.destroy();
            resolve([]);
        });
    });
}

// --- CDP Logic ---

async function connectCDP(url) {
    const ws = new WebSocket(url);
    await new Promise((resolve, reject) => {
        ws.on('open', resolve);
        ws.on('error', reject);
    });

    let idCounter = 1;
    const call = (method, params) => new Promise((resolve, reject) => {
        const id = idCounter++;
        const handler = (msg) => {
            const data = JSON.parse(msg);
            if (data.id === id) {
                ws.off('message', handler);
                if (data.error) reject(data.error);
                else resolve(data.result);
            }
        };
        ws.on('message', handler);
        ws.send(JSON.stringify({ id, method, params }));
    });

    const contexts = [];
    ws.on('message', (msg) => {
        try {
            const data = JSON.parse(msg);
            if (data.method === 'Runtime.executionContextCreated') {
                contexts.push(data.params.context);
            } else if (data.method === 'Runtime.executionContextDestroyed') {
                const idx = contexts.findIndex(c => c.id === data.params.executionContextId);
                if (idx !== -1) contexts.splice(idx, 1);
            }
        } catch (e) { }
    });

    await call("Runtime.enable", {});
    await new Promise(r => setTimeout(r, 500)); // give time for contexts to load

    return { ws, call, contexts, rootContextId: null };
}

async function extractMetadata(cdp) {
    const SCRIPT = `(() => {
        const root = document.getElementById('cascade') || document.getElementById('root');
        if (!root) return { found: false };

        let chatTitle = document.title || 'Agent';
        chatTitle = chatTitle.replace(/\\s*[-|]\\s*Antigravity.*$/i, '').trim() || 'Agent';

        // Extract sidebar structure (Projects and their nested chats)
        const sidebarProjects = [];
        try {
            const headers = document.getElementsByClassName('text-sm font-medium truncate m-0');
            for (let i = 0; i < headers.length; i++) {
                const h = headers[i];
                const parent = h.closest('.flex-col');
                const chatItems = parent ? Array.from(parent.querySelectorAll('.select-none.cursor-pointer')).map(el => {
                    const span = el.querySelector('span');
                    // Check if chat has a spinner or is currently active/running
                    const hasSpinner = !!el.querySelector('.animate-spin, svg[class*="spin"], [class*="spinner"]');
                    const text = span ? span.innerText : el.innerText;
                    // Clean title from duration suffixes e.g., "\n5m"
                    const cleanTitle = text.split('\n')[0].trim();
                    
                    return {
                        title: cleanTitle,
                        active: el.className.includes('bg-sidebar-muted') || el.className.includes('bg-sidebar-secondary') || el.className.includes('bg-muted'),
                        inProgress: hasSpinner
                    };
                }).filter(x => x.title && x.title !== "New Conversation" && x.title !== "Conversation History" && x.title !== "Scheduled Tasks" && !x.title.startsWith("See all")) : [];

                // Deduplicate sidebarProjects to prevent duplicates
                if (h.innerText && !sidebarProjects.some(sp => sp.project === h.innerText)) {
                    sidebarProjects.push({
                        project: h.innerText,
                        chats: chatItems
                    });
                }
            }
        } catch(e) {}

        return {
            found: true,
            chatTitle: chatTitle,
            isActive: document.hasFocus(),
            sidebarProjects: sidebarProjects
        };
    })()`;

    // Try finding context first if not known
    if (cdp.rootContextId) {
        try {
            const res = await cdp.call("Runtime.evaluate", { expression: SCRIPT, returnByValue: true, contextId: cdp.rootContextId });
            if (res.result?.value?.found) return { ...res.result.value, contextId: cdp.rootContextId };
        } catch (e) { cdp.rootContextId = null; } // reset if stale
    }

    // Search all contexts
    for (const ctx of cdp.contexts) {
        try {
            const result = await cdp.call("Runtime.evaluate", { expression: SCRIPT, returnByValue: true, contextId: ctx.id });
            if (result.result?.value?.found) {
                return { ...result.result.value, contextId: ctx.id };
            }
        } catch (e) { }
    }

    // Fallback: if we have any context, just use the page title
    const fallbackCtx = cdp.contexts[0];
    if (fallbackCtx) {
        try {
            const res = await cdp.call("Runtime.evaluate", { expression: '({found:true, chatTitle: document.title || "Agent", isActive: document.hasFocus(), sidebarProjects: []})', returnByValue: true, contextId: fallbackCtx.id });
            if (res.result?.value) return { ...res.result.value, contextId: fallbackCtx.id };
        } catch(e) {}
    }
    return null;
}

async function switchConversation(cdp, chatTitle) {
    const SCRIPT = `(() => {
        const items = Array.from(document.getElementsByTagName('span'));
        for (let i = 0; i < items.length; i++) {
            if (items[i].innerText.trim() === "${chatTitle.replace(/"/g, '\\"')}") {
                let p = items[i];
                while (p && p.tagName !== 'BODY') {
                    if (p.className.includes('cursor-pointer')) {
                        p.click();
                        return { success: true };
                    }
                    p = p.parentElement;
                }
            }
        }
        return { success: false, reason: 'Element not found or not clickable' };
    })()`;
    try {
        const contextId = cdp.rootContextId || (cdp.contexts[0]?.id);
        if (!contextId) return { success: false, reason: 'No context id' };
        const res = await cdp.call("Runtime.evaluate", { expression: SCRIPT, returnByValue: true, contextId });
        return res.result?.value || { success: false };
    } catch(e) {
        return { success: false, reason: e.message };
    }
}

async function captureCSS(cdp) {
    const SCRIPT = `(() => {
        let css = '';
        for (const sheet of document.styleSheets) {
            try { 
                for (const rule of sheet.cssRules) {
                    let text = rule.cssText;
                    // Scope body/html rules to avoid polluting the monitor UI
                    text = text.replace(/(^|[\\s,}])body(?=[\\s,{])/gi, '$1#ag-mirror');
                    text = text.replace(/(^|[\\s,}])html(?=[\\s,{])/gi, '$1#ag-mirror');
                    css += text + '\\n'; 
                }
            } catch (e) { }
        }
        return { css };
    })()`;

    const contextId = cdp.rootContextId || (cdp.contexts[0]?.id);
    if (!contextId) return '';

    try {
        const result = await cdp.call("Runtime.evaluate", {
            expression: SCRIPT,
            returnByValue: true,
            contextId: contextId
        });
        return result.result?.value?.css || '';
    } catch (e) { return ''; }
}

async function captureHTML(cdp) {
    const SCRIPT = `(() => {
        // Old #cascade style support
        const cascade = document.getElementById('cascade');
        if (cascade) {
            const clone = cascade.cloneNode(true);
            clone.querySelectorAll('[contenteditable="true"]').forEach(el => el.closest('div')?.remove());
            clone.id = 'ag-mirror';
            const bg = window.getComputedStyle(document.body);
            return { html: clone.outerHTML, bodyBg: bg.backgroundColor, bodyColor: bg.color };
        }

        // New Antigravity: find the scrollable chat messages container
        // It's the div with scrollbar-hide that has the most text content
        const chatEl = Array.from(document.querySelectorAll('div')).filter(d => {
            const s = window.getComputedStyle(d);
            return (s.overflowY === 'auto' || s.overflowY === 'scroll')
                && d.scrollHeight > 500
                && (d.scrollHeight / Math.max(d.clientHeight, 1)) > 1.1;
        }).sort((a, b) => (b.innerText?.length || 0) - (a.innerText?.length || 0))[0];

        if (!chatEl) return { error: 'chat element not found' };

        const clone = chatEl.cloneNode(true);
        // Remove any lingering input areas
        clone.querySelectorAll('[contenteditable="true"]').forEach(el => {
            const p = el.parentElement; if (p) p.remove();
        });
        clone.querySelectorAll('[id*="InputBox"],[id*="inputBox"]').forEach(el => {
            const p = el.closest('div[class*="flex-shrink"]') || el.parentElement;
            if (p) p.remove();
        });

        // Wrap with 'dark' class so Tailwind dark-mode CSS vars activate
        const wrapper = document.createElement('div');
        wrapper.id = 'ag-mirror';
        wrapper.className = 'dark';
        wrapper.appendChild(clone);

        const bg = window.getComputedStyle(document.body);
        return {
            html: wrapper.outerHTML,
            bodyBg: bg.backgroundColor,
            bodyColor: bg.color
        };
    })()`;

    const contextId = cdp.rootContextId || (cdp.contexts[0]?.id);
    if (!contextId) return null;

    try {
        const result = await cdp.call("Runtime.evaluate", {
            expression: SCRIPT,
            returnByValue: true,
            contextId: contextId
        });
        if (result.result?.value && !result.result.value.error) {
            return result.result.value;
        }
    } catch (e) { }
    return null;
}

// --- Main App Logic ---

async function discover() {
    // 1. Find all targets
    const allTargets = [];
    await Promise.all(PORTS.map(async (port) => {
        const list = await getJson(`http://127.0.0.1:${port}/json/list`);
        const workbenches = list.filter(t => 
            t.type === 'page' && (
                t.url?.includes('workbench.html') || 
                t.title?.toLowerCase().includes('workbench') ||
                t.title?.toLowerCase().includes('antigravity') ||
                t.url?.includes('/c/') ||
                t.url?.includes('127.0.0.1') ||
                t.url?.includes('localhost')
            )
        );
        workbenches.forEach(t => allTargets.push({ ...t, port }));
    }));

    const newCascades = new Map();

    // 2. Connect/Refresh
    for (const target of allTargets) {
        const id = hashString(target.webSocketDebuggerUrl);

        // Reuse existing
        if (cascades.has(id)) {
            const existing = cascades.get(id);
            if (existing.cdp.ws.readyState === WebSocket.OPEN) {
                // Refresh metadata
                const meta = await extractMetadata(existing.cdp);
                if (meta) {
                    existing.metadata = { ...existing.metadata, ...meta };
                    if (meta.contextId) existing.cdp.rootContextId = meta.contextId; // Update optimization
                    newCascades.set(id, existing);
                    continue;
                }
            }
        }

        // New connection
        try {
            console.log(`🔌 Connecting to ${target.title}`);
            const cdp = await connectCDP(target.webSocketDebuggerUrl);
            const meta = await extractMetadata(cdp);

            if (meta) {
                if (meta.contextId) cdp.rootContextId = meta.contextId;
                const cascade = {
                    id,
                    cdp,
                    metadata: {
                        windowTitle: target.title,
                        chatTitle: meta.chatTitle,
                        isActive: meta.isActive,
                        sidebarProjects: meta.sidebarProjects || []
                    },
                    snapshot: null,
                    css: await captureCSS(cdp), //only on init bc its huge
                    snapshotHash: null
                };
                newCascades.set(id, cascade);
                console.log(`✨ Added cascade: ${meta.chatTitle}`);
            } else {
                cdp.ws.close();
            }
        } catch (e) {
            // console.error(`Failed to connect to ${target.title}: ${e.message}`);
        }
    }

    // 3. Cleanup old
    for (const [id, c] of cascades.entries()) {
        if (!newCascades.has(id)) {
            console.log(`👋 Removing cascade: ${c.metadata.chatTitle}`);
            try { c.cdp.ws.close(); } catch (e) { }
        }
    }

    const changed = cascades.size !== newCascades.size; // Simple check, could be more granular
    cascades = newCascades;

    if (changed) broadcastCascadeList();
}

async function updateSnapshots() {
    // Parallel updates
    await Promise.all(Array.from(cascades.values()).map(async (c) => {
        try {
            // Also refresh projects/conversations metadata in sync loop (every 500ms)
            const meta = await extractMetadata(c.cdp);
            if (meta) {
                const oldProjectsStr = JSON.stringify(c.metadata.sidebarProjects || []);
                const newProjectsStr = JSON.stringify(meta.sidebarProjects || []);
                c.metadata = { ...c.metadata, ...meta };
                
                if (oldProjectsStr !== newProjectsStr) {
                    broadcastCascadeList();
                }
            }

            const snap = await captureHTML(c.cdp); // Only capture HTML
            if (snap) {
                const hash = hashString(snap.html);
                if (hash !== c.snapshotHash) {
                    c.snapshot = snap;
                    c.snapshotHash = hash;
                    broadcast({ type: 'snapshot_update', cascadeId: c.id });
                    // console.log(`📸 Updated ${c.metadata.chatTitle}`);
                }
            }
        } catch (e) { }
    }));
}

function broadcast(msg) {
    if (!wss) return;
    wss.clients.forEach(c => {
        if (c.readyState === WebSocket.OPEN) c.send(JSON.stringify(msg));
    });
}

function broadcastCascadeList() {
    const list = Array.from(cascades.values()).map(c => ({
        id: c.id,
        title: c.metadata.chatTitle,
        window: c.metadata.windowTitle,
        active: c.metadata.isActive,
        projects: c.metadata.sidebarProjects || []
    }));
    broadcast({ type: 'cascade_list', cascades: list });
}

// --- Server Setup ---

async function main() {
    const app = express();
    const server = http.createServer(app);
    wss = new WebSocketServer({ server });

    app.use(express.json());
    app.use(express.static(join(__dirname, 'public')));

    // API Routes
    app.get('/cascades', (req, res) => {
        res.json(Array.from(cascades.values()).map(c => ({
            id: c.id,
            title: c.metadata.chatTitle,
            active: c.metadata.isActive,
            projects: c.metadata.sidebarProjects || []
        })));
    });

    app.post('/switch/:id', async (req, res) => {
        const c = cascades.get(req.params.id);
        if (!c) return res.status(404).json({ error: 'Cascade not found' });
        const { chatTitle } = req.body;
        if (!chatTitle) return res.status(400).json({ error: 'Missing chatTitle' });

        const result = await switchConversation(c.cdp, chatTitle);
        if (result.success) {
            // Give it a brief moment to update then capture snapshot immediately
            setTimeout(async () => {
                const meta = await extractMetadata(c.cdp);
                if (meta) {
                    c.metadata = { ...c.metadata, ...meta };
                }
                const snap = await captureHTML(c.cdp);
                if (snap) {
                    c.snapshot = snap;
                    c.snapshotHash = hashString(snap.html);
                    broadcast({ type: 'snapshot_update', cascadeId: c.id });
                }
                broadcastCascadeList();
            }, 300);
            res.json({ success: true });
        } else {
            res.status(500).json(result);
        }
    });

    app.get('/snapshot/:id', async (req, res) => {
        const c = cascades.get(req.params.id);
        if (!c) return res.status(404).json({ error: 'Not found' });
        // Force a live snapshot capture on request to guarantee synced content
        const snap = await captureHTML(c.cdp);
        if (snap) {
            c.snapshot = snap;
            c.snapshotHash = hashString(snap.html);
        }
        if (!c.snapshot) return res.status(404).json({ error: 'Not found' });
        res.json(c.snapshot);
    });

    app.get('/styles/:id', (req, res) => {
        const c = cascades.get(req.params.id);
        if (!c) return res.status(404).json({ error: 'Not found' });
        res.json({ css: c.css || '' });
    });

    // Alias for simple single-view clients (returns first active or first available)
    app.get('/snapshot', (req, res) => {
        const active = Array.from(cascades.values()).find(c => c.metadata.isActive) || cascades.values().next().value;
        if (!active || !active.snapshot) return res.status(503).json({ error: 'No snapshot' });
        res.json(active.snapshot);
    });

    app.post('/send/:id', async (req, res) => {
        const c = cascades.get(req.params.id);
        if (!c) return res.status(404).json({ error: 'Cascade not found' });

        // Re-using the injection logic logic would be long, 
        // but let's assume valid injection for brevity in this single-file request:
        // We'll trust the previous logic worked, just pointing it to c.cdp

        // ... (Injection logic here would be same as before, simplified for brevity of this file edit)
        // For now, let's just log it to prove flow works
        console.log(`Message to ${c.metadata.chatTitle}: ${req.body.message}`);
        // TODO: Port the full injection script back in if needed, 
        // but user asked for "update" which implies features, I'll assume I should include it.
        // See helper below.

        const result = await injectMessage(c.cdp, req.body.message);
        if (result.ok) res.json({ success: true });
        else res.status(500).json(result);
    });


    wss.on('connection', (ws) => {
        broadcastCascadeList(); // Send list on connect
    });

    const PORT = process.env.PORT || 3000;
    server.listen(PORT, '0.0.0.0', () => {
        console.log(`🚀 Server running on port ${PORT}`);
    });

    // Start Loops
    discover();
    setInterval(discover, DISCOVERY_INTERVAL);
    setInterval(updateSnapshots, POLL_INTERVAL);
}

// Injection Helper (Moved down to keep main clear)
async function injectMessage(cdp, text) {
    const SCRIPT = `(async () => {
        // Try contenteditable first, then textarea
        const editor = document.querySelector('[contenteditable="true"]') || document.querySelector('textarea');
        if (!editor) return { ok: false, reason: "no editor found" };
        
        editor.focus();
        
        if (editor.tagName === 'TEXTAREA') {
            const nativeTextAreaValueSetter = Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, "value").set;
            nativeTextAreaValueSetter.call(editor, "${text.replace(/"/g, '\\"')}");
            editor.dispatchEvent(new Event('input', { bubbles: true }));
        } else {
            document.execCommand("selectAll", false, null);
            document.execCommand("insertText", false, "${text.replace(/"/g, '\\"')}");
        }
        
        await new Promise(r => setTimeout(r, 100));
        
        // Try multiple button selectors
        const btn = document.querySelector('button[class*="arrow"]') || 
                   document.querySelector('button[aria-label*="Send"]') ||
                   document.querySelector('button[type="submit"]');

        if (btn) {
            btn.click();
        } else {
             // Fallback to Enter key
             editor.dispatchEvent(new KeyboardEvent("keydown", { bubbles:true, key:"Enter" }));
        }
        return { ok: true };
    })()`;

    try {
        const res = await cdp.call("Runtime.evaluate", {
            expression: SCRIPT,
            returnByValue: true,
            contextId: cdp.rootContextId
        });
        return res.result?.value || { ok: false };
    } catch (e) { return { ok: false, reason: e.message }; }
}

main();
